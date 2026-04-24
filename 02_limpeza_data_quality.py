# SCRIPT: 02_limpeza_data_quality.py
# ETAPA: Processamento Vetorizado, Data Quality e Enriquecimento Analítico

import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def executar_pipeline():
    logging.info("Carregando camada bronze...")
    df = pd.read_csv('backlog_garantias_raw.csv')

    # Validação básica de schema (garante integridade do pipeline)
    colunas_esperadas = [
        'status_sistema',
        'prioridade_auditoria',
        'data_abertura',
        'valor_pleiteado',
        'doc_contabil_sap',
        'dias_em_aberto'
    ]

    faltantes = [col for col in colunas_esperadas if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no dataset: {faltantes}")

    # 1. Sanitização e Tipagem
    logging.info("Aplicando padronizações de tipos e strings...")

    df['status_sistema'] = df['status_sistema'].astype(str).str.strip().str.upper()
    df['prioridade_auditoria'] = df['prioridade_auditoria'].astype(str).str.strip().str.upper()

    df['data_abertura'] = pd.to_datetime(
        df['data_abertura'],
        format='mixed',
        dayfirst=True,
        errors='coerce'
    )

    # 2. Normalização Financeira
    logging.info("Tratando valores financeiros...")

    df['valor_pleiteado'] = pd.to_numeric(df['valor_pleiteado'], errors='coerce')

    # Remoção de valores não auditáveis
    df = df.loc[df['valor_pleiteado'] > 0].copy()

    # 3. Identificação de Falhas de Integração SAP
    logging.info("Identificando falhas de integração com SAP...")

    sap_fail_mask = (
        df['doc_contabil_sap'].isna() |
        (df['doc_contabil_sap'] == 'ERRO_SAP')
    )

    df['status_integracao'] = np.where(
        sap_fail_mask,
        'FALHA_INTEGRACAO',
        'INTEGRADO'
    )

    # 4. Cálculo de Risco Financeiro (vetorizado)
    logging.info("Calculando risco financeiro com base no aging...")

    df['risco_perda_financeira'] = np.where(
        df['dias_em_aberto'] > 365,
        df['valor_pleiteado'] * 1.15,
        df['valor_pleiteado']
    )

    # 5. Score de Prioridade (integração com regra do SQL)
    logging.info("Calculando score de risco baseado na prioridade de auditoria...")

    mapa_prioridade = {
        'CRITICO': 3,
        'ALTA': 2,
        'NORMAL': 1
    }

    df['peso_prioridade'] = df['prioridade_auditoria'].map(mapa_prioridade).fillna(1)

    df['score_risco'] = df['risco_perda_financeira'] * df['peso_prioridade']

    # 6. Data Quality Report
    logging.info("Gerando métricas de qualidade de dados...")

    total_registros = len(df)
    falhas_integracao = df['status_integracao'].eq('FALHA_INTEGRACAO').sum()
    taxa_falha_integracao = (
        falhas_integracao / total_registros if total_registros > 0 else 0
    )

    logging.info(f"Total de registros válidos: {total_registros}")
    logging.info(f"Falhas de integração: {falhas_integracao}")
    logging.info(f"Taxa de falha de integração: {taxa_falha_integracao:.2%}")

    # 7. Persistência da Camada Prata
    logging.info("Salvando camada silver...")

    df.to_csv('garantias_auditadas_silver.csv', index=False)

    # 8. Sumário Executivo
    total_recuperavel = df.loc[
        df['status_integracao'] == 'FALHA_INTEGRACAO',
        'valor_pleiteado'
    ].sum()

    logging.info(
        f"Pipeline concluído. Volume recuperável identificado: R$ {total_recuperavel:,.2f}"
    )


if __name__ == "__main__":
    executar_pipeline()