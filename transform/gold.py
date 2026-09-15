import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("transform.gold")


def criar_gold_top_clientes(spark):
    """
    Top 10 clientes por volume de eventos nos últimos 30 dias (referência:
    MAX(occurred_at) da Silver, não CURRENT_DATE — dataset é sintético e fixo).
    Enriquece com company_name/plan/segment na data do evento via SCD2.
    """
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


def criar_gold_tempo_resposta(spark):
    """
    Tempo médio até a primeira resposta (ticket_opened → primeiro
    ticket_replied do mesmo ticket_id), por plano e por mês. Tickets sem
    resposta são reportados explicitamente (LEFT JOIN), nunca descartados.
    """
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


def criar_gold_retencao_coorte(spark):
    """
    Percentual de clientes com pelo menos 1 evento no mês, agrupados pelo mês
    de signup_date (coorte de aquisição).
    """
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


def run(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    criar_gold_top_clientes(spark)
    criar_gold_tempo_resposta(spark)
    criar_gold_retencao_coorte(spark)
    logger.info("gold_completed")


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    with SparkSession.builder.appName("gold").getOrCreate() as spark:
        run(spark)
