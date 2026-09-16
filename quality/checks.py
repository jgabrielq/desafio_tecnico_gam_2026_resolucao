import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("quality.checks")


def persistir_resultado(spark, check_name, check_type, severity, status,
                         table_name, metric_value, threshold_value,
                         details, batch_id):
    from datetime import datetime, timezone
    row = [(
        check_name, check_type, severity, status,
        table_name, float(metric_value), float(threshold_value),
        details, batch_id, datetime.now(timezone.utc)
    )]
    schema = """
        check_name STRING, check_type STRING, severity STRING, status STRING,
        table_name STRING, metric_value DOUBLE, threshold_value DOUBLE,
        details STRING, batch_id STRING, executed_at TIMESTAMP
    """
    df = spark.createDataFrame(row, schema=schema)
    df.writeTo("lakehouse.quality.check_results").append()


def check_unicidade_event_id(spark, batch_id):
    """Check 1 — Unicidade da chave primária (event_id) — BLOCKING."""
    result = spark.sql("""
        SELECT COUNT(*) - COUNT(DISTINCT event_id) AS duplicatas
        FROM lakehouse.silver.events
    """).collect()[0]["duplicatas"]

    status = "PASSED" if result == 0 else "FAILED"
    persistir_resultado(
        spark,
        check_name="unicidade_event_id",
        check_type="unicidade",
        severity="BLOCKING",
        status=status,
        table_name="lakehouse.silver.events",
        metric_value=result,
        threshold_value=0,
        details=f"{result} event_ids duplicados encontrados na silver.events",
        batch_id=batch_id
    )
    return status


def check_integridade_referencial(spark, batch_id):
    """Check 2 — Integridade referencial — WARNING."""
    result = spark.sql("""
        SELECT COUNT(*) AS orfaos
        FROM lakehouse.silver.events e
        LEFT JOIN lakehouse.silver.customers c
            ON e.customer_id = c.customer_id
            AND c.is_current = true
        WHERE e.customer_id IS NOT NULL
          AND c.customer_id IS NULL
    """).collect()[0]["orfaos"]

    status = "PASSED" if result == 0 else "FAILED"
    persistir_resultado(
        spark,
        check_name="integridade_referencial_customer_id",
        check_type="integridade_referencial",
        severity="WARNING",
        status=status,
        table_name="lakehouse.silver.events",
        metric_value=result,
        threshold_value=0,
        details=f"{result} eventos com customer_id sem correspondência em silver.customers",
        batch_id=batch_id
    )
    return status


def check_volumetria(spark, batch_id):
    """Check 3 — Volumetria fora do esperado — WARNING."""
    volume_atual = spark.sql(f"""
        SELECT COUNT(*) AS total
        FROM lakehouse.silver.events
        WHERE _batch_id = '{batch_id}'
    """).collect()[0]["total"]

    media_historica = spark.sql(f"""
        SELECT AVG(total) AS media
        FROM (
            SELECT _batch_id, COUNT(*) AS total
            FROM lakehouse.silver.events
            WHERE _batch_id != '{batch_id}'
            GROUP BY _batch_id
            ORDER BY _batch_id DESC
            LIMIT 7
        )
    """).collect()[0]["media"]

    if media_historica is None or media_historica == 0:
        persistir_resultado(
            spark,
            check_name="volumetria_events",
            check_type="volumetria",
            severity="WARNING",
            status="PASSED",
            table_name="lakehouse.silver.events",
            metric_value=volume_atual,
            threshold_value=0,
            details="Sem histórico suficiente para comparação de volumetria",
            batch_id=batch_id
        )
        return "PASSED"

    variacao_pct = abs(volume_atual - media_historica) / media_historica * 100
    threshold = 50.0
    status = "PASSED" if variacao_pct <= threshold else "FAILED"

    persistir_resultado(
        spark,
        check_name="volumetria_events",
        check_type="volumetria",
        severity="WARNING",
        status=status,
        table_name="lakehouse.silver.events",
        metric_value=round(variacao_pct, 2),
        threshold_value=threshold,
        details=(f"Volume atual: {volume_atual} | "
                 f"Media historica: {media_historica:.0f} | "
                 f"Variacao: {variacao_pct:.1f}%"),
        batch_id=batch_id
    )
    return status


def check_dominios(spark, batch_id):
    """Check 4 — Valores de domínio inválidos — WARNING."""
    # Lista de domínio ajustada: a spec original não incluía 'feature_used',
    # que é um event_type legítimo do dataset (ver AJUSTES_PARTE4_QUALITY.md).
    event_types_validos = [
        'login', 'logout', 'page_view', 'purchase', 'ticket_opened',
        'ticket_replied', 'ticket_closed', 'export_generated',
        'report_viewed', 'api_call', 'feature_used'
    ]
    event_types_str = ", ".join(f"'{t}'" for t in event_types_validos)

    invalidos_events = spark.sql(f"""
        SELECT COUNT(*) AS total
        FROM lakehouse.silver.events
        WHERE event_type NOT IN ({event_types_str})
          AND event_type IS NOT NULL
    """).collect()[0]["total"]

    planos_str = "'free', 'pro', 'enterprise'"
    invalidos_plans = spark.sql(f"""
        SELECT COUNT(*) AS total
        FROM lakehouse.silver.customers
        WHERE plan NOT IN ({planos_str})
          AND plan IS NOT NULL
          AND is_current = true
    """).collect()[0]["total"]

    total_invalidos = invalidos_events + invalidos_plans
    status = "PASSED" if total_invalidos == 0 else "FAILED"

    persistir_resultado(
        spark,
        check_name="dominios_invalidos",
        check_type="dominio",
        severity="WARNING",
        status=status,
        table_name="lakehouse.silver.events,lakehouse.silver.customers",
        metric_value=total_invalidos,
        threshold_value=0,
        details=(f"{invalidos_events} event_types invalidos | "
                 f"{invalidos_plans} planos invalidos"),
        batch_id=batch_id
    )
    return status


def check_freshness(spark, batch_id):
    """
    Check 5 — Freshness — BLOCKING.

    Ajuste em relação à spec original: a defasagem é medida entre o
    `ingestion_date` do batch (a data que o pipeline considera "hoje") e o
    MAX(occurred_at) DENTRO DESSE BATCH — não contra o relógio real da
    máquina que rodou o job (`_ingested_at`). O dataset é sintético e fixo
    no tempo (mesmo princípio da Gold: MAX(occurred_at), não CURRENT_DATE);
    comparar contra o relógio real faria este check BLOCKING falhar de forma
    permanente conforme os dias reais passam, mesmo sem nenhum atraso
    genuíno no pipeline. Ver AJUSTES_PARTE4_QUALITY.md.
    """
    result = spark.sql(f"""
        SELECT
            MAX(occurred_at) AS max_occurred_at,
            DATEDIFF(DATE('{batch_id}'), MAX(occurred_at)) AS defasagem_dias
        FROM lakehouse.silver.events
        WHERE _batch_id = '{batch_id}'
    """).collect()[0]

    defasagem = result["defasagem_dias"] if result["defasagem_dias"] is not None else 0
    threshold = 2
    status = "PASSED" if defasagem <= threshold else "FAILED"

    persistir_resultado(
        spark,
        check_name="freshness_events",
        check_type="freshness",
        severity="BLOCKING",
        status=status,
        table_name="lakehouse.silver.events",
        metric_value=defasagem,
        threshold_value=threshold,
        details=(f"Batch ingestion_date: {batch_id} | "
                 f"Max occurred_at do batch: {result['max_occurred_at']} | "
                 f"Defasagem: {defasagem} dias"),
        batch_id=batch_id
    )
    return status


def run(ingestion_date: str, spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.quality")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.quality.check_results (
            check_name STRING, check_type STRING, severity STRING,
            status STRING, table_name STRING, metric_value DOUBLE,
            threshold_value DOUBLE, details STRING,
            batch_id STRING, executed_at TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """)

    resultados = {}
    resultados["unicidade"] = check_unicidade_event_id(spark, ingestion_date)
    resultados["referencial"] = check_integridade_referencial(spark, ingestion_date)
    resultados["volumetria"] = check_volumetria(spark, ingestion_date)
    resultados["dominios"] = check_dominios(spark, ingestion_date)
    resultados["freshness"] = check_freshness(spark, ingestion_date)

    checks_blocking = ["unicidade", "freshness"]
    falhas_blocking = [n for n in checks_blocking if resultados.get(n) == "FAILED"]

    passou = sum(1 for s in resultados.values() if s == "PASSED")
    falhou = sum(1 for s in resultados.values() if s == "FAILED")
    logger.info(f"Quality checks: {passou} PASSED | {falhou} FAILED")

    if falhas_blocking:
        raise RuntimeError(
            f"Checks BLOCKING falharam: {falhas_blocking}. "
            "Pipeline interrompido. Verifique lakehouse.quality.check_results."
        )


if __name__ == "__main__":
    import argparse
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion_date", required=True)
    args = parser.parse_args()

    with SparkSession.builder \
        .appName(f"quality_checks_{args.ingestion_date}") \
        .getOrCreate() as spark:

        run(args.ingestion_date, spark)
