# SPEC — Parte 3: Gold + Trino

Especificação técnica completa para implementação da camada Gold e das 3 queries analíticas.
Leia este arquivo inteiro antes de escrever qualquer código.
Implemente APENAS após Bronze e Silver estarem validadas.

---

## Contexto e decisões arquiteturais

**Tabelas Gold materializadas** (não views): pré-calculadas via PySpark, servem
múltiplas queries com performance previsível. Permitem adicionar `_generated_at`
como coluna de auditoria.

**Um único job** `transform/gold.py` cria e popula todas as tabelas Gold.

**Definição de "últimos 30 dias"**: usa `MAX(occurred_at)` da Silver como
referência — não `CURRENT_DATE`. O dataset é sintético e fixo; `CURRENT_DATE`
retornaria zero resultados após a data máxima dos dados.

**Namespace**: `lakehouse.gold`

---

## Estrutura de arquivos a criar

```
transform/
└── gold.py          # job PySpark: silver → lakehouse.gold.*

sql/
├── query1_top10_clientes.sql          # query Trino + resultado CSV
├── query2_tempo_resposta_ticket.sql   # query Trino + resultado CSV
├── query3_retencao_coorte.sql         # query Trino + resultado CSV
└── resultados/
    ├── query1_resultado.csv
    ├── query2_resultado.csv
    └── query3_resultado.csv
```

---

## `transform/gold.py`

### Ponto de entrada
```bash
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/gold.py
```
Não recebe `ingestion_date` — Gold é sempre recalculada do estado completo da Silver.

### Estrutura geral do job

```python
def run(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    criar_gold_top_clientes(spark)
    criar_gold_tempo_resposta(spark)
    criar_gold_retencao_coorte(spark)
    logger.info("gold_completed")

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("gold").getOrCreate()
    try:
        run(spark)
    finally:
        spark.stop()
```

---

### Tabela 1 — `lakehouse.gold.top_clientes_30d`

**Objetivo**: top 10 clientes por volume de eventos nos últimos 30 dias,
com nome da empresa, plano e segmento.

**Lógica**:
```python
def criar_gold_top_clientes(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.top_clientes_30d (
            customer_id   STRING,
            company_name  STRING,
            plan          STRING,
            segment       STRING,
            total_eventos BIGINT,
            _generated_at TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """)

    # Overwrite completo — Gold é sempre recalculada
    spark.sql("""
        DELETE FROM lakehouse.gold.top_clientes_30d WHERE 1=1
    """)

    spark.sql("""
        INSERT INTO lakehouse.gold.top_clientes_30d
        SELECT
            e.customer_id,
            c.company_name,
            c.plan,
            c.segment,
            COUNT(*) AS total_eventos,
            CURRENT_TIMESTAMP AS _generated_at
        FROM lakehouse.silver.events e
        JOIN lakehouse.silver.customers c
            ON e.customer_id = c.customer_id
            AND e.occurred_at BETWEEN c.valid_from AND c.valid_to
        WHERE
            e.occurred_at >= (
                SELECT MAX(occurred_at) - INTERVAL 30 DAYS
                FROM lakehouse.silver.events
            )
            AND e._customer_exists = true
        GROUP BY e.customer_id, c.company_name, c.plan, c.segment
        ORDER BY total_eventos DESC
        LIMIT 10
    """)
```

**Decisões importantes**:
- `JOIN ... ON e.occurred_at BETWEEN c.valid_from AND c.valid_to`: usa o SCD2
  para pegar o plano do cliente NA DATA do evento, não o plano atual.
- `e._customer_exists = true`: exclui eventos órfãos (sem customer_id) da
  contagem — eles não têm empresa/plano para enriquecer.
- `MAX(occurred_at) - INTERVAL 30 DAYS`: referência dinâmica ao último dia
  com dados, não `CURRENT_DATE`.

---

### Tabela 2 — `lakehouse.gold.tempo_resposta_ticket`

**Objetivo**: tempo médio até a primeira resposta (ticket_opened → primeiro
ticket_replied do mesmo ticket_id), por plano e por mês. Tickets sem resposta
tratados explicitamente.

**Lógica**:
```python
def criar_gold_tempo_resposta(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.tempo_resposta_ticket (
            plan                  STRING,
            mes                   STRING,
            avg_minutos_resposta  DOUBLE,
            tickets_com_resposta  BIGINT,
            tickets_sem_resposta  BIGINT,
            _generated_at         TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """)

    spark.sql("DELETE FROM lakehouse.gold.tempo_resposta_ticket WHERE 1=1")

    spark.sql("""
        INSERT INTO lakehouse.gold.tempo_resposta_ticket
        WITH aberturas AS (
            SELECT
                get_json_object(properties, '$.ticket_id') AS ticket_id,
                occurred_at AS aberto_em,
                customer_id
            FROM lakehouse.silver.events
            WHERE event_type = 'ticket_opened'
        ),
        respostas AS (
            SELECT
                get_json_object(properties, '$.ticket_id') AS ticket_id,
                MIN(occurred_at) AS primeiro_reply_em
            FROM lakehouse.silver.events
            WHERE event_type = 'ticket_replied'
            GROUP BY get_json_object(properties, '$.ticket_id')
        ),
        joined AS (
            SELECT
                a.ticket_id,
                a.aberto_em,
                a.customer_id,
                r.primeiro_reply_em,
                CASE
                    WHEN r.primeiro_reply_em IS NOT NULL
                    THEN (unix_timestamp(r.primeiro_reply_em) - unix_timestamp(a.aberto_em)) / 60.0
                    ELSE NULL
                END AS minutos_ate_resposta
            FROM aberturas a
            LEFT JOIN respostas r ON a.ticket_id = r.ticket_id
        )
        SELECT
            c.plan,
            DATE_FORMAT(j.aberto_em, 'yyyy-MM') AS mes,
            ROUND(AVG(j.minutos_ate_resposta), 2) AS avg_minutos_resposta,
            COUNT(CASE WHEN j.minutos_ate_resposta IS NOT NULL THEN 1 END) AS tickets_com_resposta,
            COUNT(CASE WHEN j.minutos_ate_resposta IS NULL THEN 1 END) AS tickets_sem_resposta,
            CURRENT_TIMESTAMP AS _generated_at
        FROM joined j
        JOIN lakehouse.silver.customers c
            ON j.customer_id = c.customer_id
            AND j.aberto_em BETWEEN c.valid_from AND c.valid_to
        GROUP BY c.plan, DATE_FORMAT(j.aberto_em, 'yyyy-MM')
        ORDER BY mes, plan
    """)
```

**Decisões importantes**:
- `get_json_object(properties, '$.ticket_id')`: extrai o `ticket_id` do campo
  `properties` (que é STRING na Silver). Evita precisar explodir o JSON inteiro.
- `LEFT JOIN respostas`: garante que tickets sem resposta aparecem no resultado
  com `primeiro_reply_em = NULL` — não são descartados silenciosamente.
- `AVG` ignora NULL automaticamente em SQL — `avg_minutos_resposta` representa
  apenas os tickets que tiveram resposta. A coluna `tickets_sem_resposta` torna
  isso explícito para o avaliador.
- `unix_timestamp` para calcular diferença em segundos, dividido por 60 para
  minutos — mais portável que `TIMESTAMPDIFF` (não disponível no Trino).
- JOIN com SCD2 de customers na data de abertura do ticket.

---

### Tabela 3 — `lakehouse.gold.retencao_coorte`

**Objetivo**: percentual de clientes com pelo menos 1 evento no mês, agrupados
pelo mês de `signup_date` (coorte de aquisição).

**Lógica**:
```python
def criar_gold_retencao_coorte(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.gold.retencao_coorte (
            cohort_month        STRING,
            activity_month      STRING,
            clientes_na_coorte  BIGINT,
            clientes_ativos     BIGINT,
            retencao_pct        DOUBLE,
            _generated_at       TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """)

    spark.sql("DELETE FROM lakehouse.gold.retencao_coorte WHERE 1=1")

    spark.sql("""
        INSERT INTO lakehouse.gold.retencao_coorte
        WITH coortes AS (
            -- Um registro por cliente com seu mês de signup (coorte)
            SELECT
                customer_id,
                DATE_FORMAT(signup_date, 'yyyy-MM') AS cohort_month
            FROM lakehouse.silver.customers
            WHERE is_current = true
        ),
        atividade AS (
            -- Um registro por cliente/mês com atividade
            SELECT DISTINCT
                customer_id,
                DATE_FORMAT(occurred_at, 'yyyy-MM') AS activity_month
            FROM lakehouse.silver.events
            WHERE _customer_exists = true
        ),
        tamanho_coorte AS (
            -- Quantos clientes em cada coorte
            SELECT cohort_month, COUNT(DISTINCT customer_id) AS clientes_na_coorte
            FROM coortes
            GROUP BY cohort_month
        )
        SELECT
            c.cohort_month,
            a.activity_month,
            t.clientes_na_coorte,
            COUNT(DISTINCT c.customer_id) AS clientes_ativos,
            ROUND(
                100.0 * COUNT(DISTINCT c.customer_id) / t.clientes_na_coorte,
                2
            ) AS retencao_pct,
            CURRENT_TIMESTAMP AS _generated_at
        FROM coortes c
        JOIN atividade a ON c.customer_id = a.customer_id
        JOIN tamanho_coorte t ON c.cohort_month = t.cohort_month
        GROUP BY c.cohort_month, a.activity_month, t.clientes_na_coorte
        ORDER BY cohort_month, activity_month
    """)
```

**Decisões importantes**:
- `is_current = true` em coortes: usa apenas a versão atual do cliente para
  determinar o `signup_date` — o signup não muda, então isso é correto e
  evita duplicatas de clientes com histórico SCD2.
- `DISTINCT customer_id` em atividade: um cliente com múltiplos eventos no
  mesmo mês conta uma única vez para a retenção.
- `_customer_exists = true` em atividade: exclui eventos órfãos.
- A divisão `100.0 * COUNT / total` usa `100.0` (float) para evitar divisão
  inteira — o mesmo problema que vimos nos exercícios de SQL.

---

## Como executar o job Gold

```bash
# Copiar transform/ atualizado para o container
docker cp transform dl-spark:/tmp/transform

# Rodar o job Gold
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/gold.py
```

---

## Queries Trino para validação e entrega

Após o job Gold rodar, execute as queries abaixo no Trino e salve os resultados
em `sql/resultados/`. As queries de entrega são mais simples que a lógica do job
(já estão materializadas) — só leem as tabelas Gold.

### `sql/query1_top10_clientes.sql`

```sql
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
```

**Interpretação a escrever** (1 parágrafo):
- Quais planos dominam o top 10? (enterprise, pro, free?)
- Existe concentração de eventos em poucos clientes?
- O resultado faz sentido dado o que sabemos sobre o dataset?

### `sql/query2_tempo_resposta_ticket.sql`

```sql
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
```

**Interpretação a escrever** (1 parágrafo):
- Clientes enterprise têm tempo de resposta menor? (SLA diferenciado?)
- Qual mês/plano tem a maior proporção de tickets sem resposta?
- O tempo médio está em minutos — converter para horas/dias se necessário
  para facilitar a interpretação.

### `sql/query3_retencao_coorte.sql`

```sql
-- Query 3: Retenção por coorte de aquisição
SELECT
    cohort_month,
    activity_month,
    clientes_na_coorte,
    clientes_ativos,
    retencao_pct
FROM lakehouse.gold.retencao_coorte
ORDER BY cohort_month, activity_month;
```

**Interpretação a escrever** (1 parágrafo):
- Coortes mais antigas têm retenção maior ou menor?
- Existe queda de retenção ao longo dos meses após o signup?
- Alguma coorte com comportamento atípico (muito alta ou muito baixa retenção)?

---

## Validação após implementação

```bash
# 1. Rodar o job Gold
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/gold.py

# 2. Confirmar tabelas criadas
docker exec dl-trino trino --execute \
    "SHOW TABLES IN lakehouse.gold"

# 3. Validar contagens
docker exec dl-trino trino --execute \
    "SELECT COUNT(*) FROM lakehouse.gold.top_clientes_30d"
# Deve retornar exatamente 10 (LIMIT 10 no job)

docker exec dl-trino trino --execute \
    "SELECT COUNT(*) FROM lakehouse.gold.tempo_resposta_ticket"

docker exec dl-trino trino --execute \
    "SELECT COUNT(*) FROM lakehouse.gold.retencao_coorte"

# 4. Rodar de novo — teste de idempotência
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/gold.py
# Contagens devem ser idênticas

# 5. Salvar resultados para entrega
docker exec dl-trino trino --output-format TSV_HEADER \
    --execute "SELECT * FROM lakehouse.gold.top_clientes_30d" \
    > sql/resultados/query1_resultado.csv

docker exec dl-trino trino --output-format TSV_HEADER \
    --execute "SELECT plan, mes, avg_minutos_resposta, tickets_com_resposta,
               tickets_sem_resposta FROM lakehouse.gold.tempo_resposta_ticket
               ORDER BY mes, plan" \
    > sql/resultados/query2_resultado.csv

docker exec dl-trino trino --output-format TSV_HEADER \
    --execute "SELECT * FROM lakehouse.gold.retencao_coorte ORDER BY
               cohort_month, activity_month" \
    > sql/resultados/query3_resultado.csv
```

---

## Notas para o code review

- **Por que materializar em vez de view**: views recalculariam do zero a cada
  query — para joins complexos com SCD2 e window functions, isso seria lento.
  Tabelas materializadas têm performance previsível e permitem auditoria via
  `_generated_at`.
- **Por que `MAX(occurred_at) - INTERVAL 30 DAYS`**: o dataset é fixo no tempo;
  `CURRENT_DATE` retornaria zero resultados.
- **Por que LEFT JOIN em tickets**: o enunciado pede "trate tickets sem resposta
  explicitamente" — INNER JOIN os descartaria silenciosamente.
- **Por que `get_json_object` e não explodir o properties**: explodir geraria
  uma linha por chave do JSON, multiplicando o volume. `get_json_object` extrai
  apenas o campo necessário com custo mínimo.
- **Por que `is_current = true` na coorte**: signup_date não muda; usar todas
  as versões SCD2 duplicaria clientes na contagem de coorte.