-- PIPELINE: Auditoria de Garantias
-- SCRIPT: 01_extracao_sap_operacoes.sql
-- ETAPA: Extração e Regras de Negócio Iniciais (Camada Bronze)

WITH SolicitacoesGarantia AS (
    SELECT 
        g.cod_garantia AS id_garantia,
        g.data_criacao AS data_abertura,
        -- Cálculo de aging: Dias entre a abertura e a data atual
        DATEDIFF(CURRENT_DATE, g.data_creacao) AS dias_em_aberto,
        eq.desc_modelo AS equipamento_modelo,
        g.valor_estimado AS valor_pleiteado,
        g.status_atual_desc AS status_sistema,
        f.tipo_falha_nome AS tipo_falha
    FROM db_operacoes.tb_garantias_historico g
    LEFT JOIN db_operacoes.tb_equipamentos eq 
        ON g.id_equipamento = eq.id_equipamento
    LEFT JOIN db_operacoes.tb_catalogo_falhas f 
        ON g.id_falha = f.id_falha
    WHERE 
        g.data_criacao >= '2023-01-01' -- Recorte temporal para auditoria de backlog
),

DocumentosSAP AS (
    SELECT 
        c.id_referencia_garantia,
        c.num_doc_contabil AS doc_contabil_sap
    FROM db_erp_sap.tb_contabilizacao_docs c
    WHERE 
        c.tipo_documento IN ('GAR', 'EST') -- Filtro por tipos de documentos de garantia
)

SELECT 
    sg.id_garantia,
    sg.data_abertura,
    sg.dias_em_aberto,
    sg.equipamento_modelo,
    sg.valor_pleiteado,
    sg.status_sistema,
    ds.doc_contabil_sap,
    sg.tipo_falha,
    -- Regra de Priorização: Define o nível de criticidade para a auditoria manual
    CASE 
        WHEN sg.dias_em_aberto > 365 AND ds.doc_contabil_sap IS NULL THEN 'CRITICO'
        WHEN ds.doc_contabil_sap IS NULL THEN 'ALTA'
        ELSE 'NORMAL'
    END AS prioridade_auditoria
FROM SolicitacoesGarantia sg
-- Mapeamento de falhas de integração através do LEFT JOIN
LEFT JOIN DocumentosSAP ds 
    ON sg.id_garantia = ds.id_referencia_garantia;