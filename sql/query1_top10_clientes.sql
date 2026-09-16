-- Query 1: Top 10 clientes por volume de eventos nos últimos 30 dias
-- Fonte: lakehouse.gold.top_clientes_30d (pré-calculada)
SELECT
    customer_id,
    company_name,
    plan,
    segment,
    total_eventos
FROM lakehouse.gold.top_clientes_30d
ORDER BY total_eventos DESC;

-- Interpretação numérica:
-- O top 10 é dominado pelo plano "pro" (8 de 10 clientes), com os 2 restantes
-- em "enterprise" — nenhum cliente "free" aparece, coerente com o fato de
-- planos pagos concentrarem uso mais intenso do produto. Os volumes variam de
-- 78 a 160 eventos por cliente (total de 1.078 eventos no top 10, contra 7.374
-- eventos totais no período de 30 dias), ou seja, uma concentração moderada
-- (~14,6% do volume) em poucos clientes, sem um outlier isolado dominando o
-- ranking — a distribuição é relativamente suave entre a 1ª e a 10ª posição.

-- Interpretação qualitativa:
-- Os resultados mostram que os clientes do plano 'pro' possuem um maior engajamento bruto - medido pela quantidade
-- absoluta de eventos realizados dentro do período. Poderíamos qualificar esse engajamento selecionando apenas os 
-- eventos relacionados ao uso saudável do produto na contagem da métrica. Com esse engajamento seletivo, poderíamos então
-- analisar a correlação com o Churn, visto que o engajamento é uma métrica conhecida que prevê o cancelamentos dos clientes