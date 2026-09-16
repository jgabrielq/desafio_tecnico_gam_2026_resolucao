-- Query 3: Retenção por coorte de aquisição
SELECT
    cohort_month,
    activity_month,
    clientes_na_coorte,
    clientes_ativos,
    retencao_pct
FROM lakehouse.gold.retencao_coorte
ORDER BY cohort_month, activity_month;

-- Interpretação numérica:
-- A retenção é alta e estável na maioria das coortes: quase todas mantêm
-- entre 85% e 100% de clientes ativos nos meses observados, sem uma queda
-- sistemática conforme o tempo passa desde o signup — coortes de 2025
-- (mais antigas) e coortes recentes de 2026 têm retenção na mesma faixa.
-- A única coorte com comportamento nitidamente atípico é a mais recente
-- (2026-08), com apenas 77,59% de retenção no único mês de atividade
-- observado até agora — mas isso é esperado, já que essa coorte teve menos
-- tempo para gerar eventos e ainda não tem dados suficientes para
-- comparação justa com as coortes mais maduras. No geral, o produto não
-- aparenta ter um problema de churn ao longo do tempo.

-- Interpretação qualitativa:
-- Esse é um resultado muito bom porque mostra que clientes adquiridos com campanhas promocionais
-- não possuem taxa de churn maiores, o que normalmente acontece quando o desconto aplicado na sua aquisição
-- expira, por exemplo.