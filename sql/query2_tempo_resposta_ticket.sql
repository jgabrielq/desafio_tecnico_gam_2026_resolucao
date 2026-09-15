-- Query 2: Tempo médio de resposta por plano e mês
-- Tickets sem resposta são reportados explicitamente
SELECT
    plan,
    mes,
    avg_minutos_resposta,
    tickets_com_resposta,
    tickets_sem_resposta,
    ROUND(
        100.0 * tickets_sem_resposta /
        NULLIF(tickets_com_resposta + tickets_sem_resposta, 0),
        1
    ) AS pct_sem_resposta
FROM lakehouse.gold.tempo_resposta_ticket
ORDER BY mes, plan;

-- Interpretação:
-- Há um SLA claramente diferenciado por plano: clientes "enterprise" recebem
-- a primeira resposta em ~31-33 minutos em média, contra ~111-122 minutos no
-- plano "pro" (3-4x mais lento) e ~312-354 minutos (5-6 horas) no plano
-- "free" (10x mais lento que enterprise) — todos os meses seguem esse mesmo
-- padrão de forma consistente. A maior proporção de tickets sem resposta é
-- em "pro" no mês 2026-08 (16,5%), seguido de perto por "enterprise" em
-- 2026-07 (15,9%); vale notar que mesmo o plano com SLA mais rápido
-- (enterprise) tem uma taxa de tickets sem resposta parecida com a dos
-- demais planos, sugerindo que a falha em responder não está relacionada
-- ao SLA prometido, e sim a uma fração de tickets que simplesmente não
-- recebe nenhuma réplica.
