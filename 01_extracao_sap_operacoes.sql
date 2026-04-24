-- PIPELINE: Auditoria de Garantias (Concessionária x Fabricante)
-- SCRIPT: 01_extracao_cat_sotreq.sql
-- ETAPA: Extração e Conciliação de Bases (Camada Bronze)
-- DESC: Realiza o cruzamento entre os registos internos da Sotreq e os dados 
--       do Portal CAT para identificar pendências de reembolso.

WITH Base_Sotreq AS (
    SELECT 
        g.cod_garantia AS id_garantia,
        g.data_criacao AS data_abertura,
        -- Cálculo de aging: Dias decorridos desde a abertura do processo
        DATEDIFF(CURRENT_DATE, g.data_criacao) AS dias_em_aberto,
        eq.desc_modelo AS equipamento_modelo,
        g.valor_estimado AS valor_pleiteado,
        g.status_atual_desc AS status_sistema,
        f.tipo_falha_nome AS tipo_falha
    FROM db_sotreq.tb_historico_garantias g
    LEFT JOIN db_sotreq.tb_frota_equipamentos eq 
        ON g.id_equipamento = eq.id_equipamento
    LEFT JOIN db_sotreq.tb_diagnostico_falhas f 
        ON g.id_falha = f.id_falha
    WHERE 
        g.data_criacao >= '2023-01-01' -- Foco no backlog acumulado a partir de 2023
),

Base_Portal_CAT AS (
    SELECT 
        p.id_referencia_sotreq,
        p.num_protocolo_fabricante AS id_processo_cat
    FROM db_fabricante.tb_portal_cat p
    WHERE 
        p.status_processamento = 'FINALIZADO'
)

SELECT 
    bs.id_garantia,
    bs.data_abertura,
    bs.dias_em_aberto,
    bs.equipamento_modelo,
    bs.valor_pleiteado,
    bs.status_sistema,
    pc.id_processo_cat,
    bs.tipo_falha,
    -- Regra de Priorização: Cruza o aging com a ausência de protocolo no portal externo
    CASE 
        WHEN bs.dias_em_aberto > 365 AND pc.id_processo_cat IS NULL THEN 'CRITICO'
        WHEN pc.id_processo_cat IS NULL THEN 'ALTA'
        ELSE 'NORMAL'
    END AS prioridade_auditoria
FROM Base_Sotreq bs
-- Identificação de hiatos através do LEFT JOIN com a base do fabricante
LEFT JOIN Base_Portal_CAT pc 
    ON bs.id_garantia = pc.id_referencia_sotreq;