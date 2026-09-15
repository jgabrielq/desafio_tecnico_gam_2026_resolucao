import logging
from datetime import datetime, timezone
from pyspark.sql import functions as F
from transform import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("transform.bronze_customers")


def run(ingestion_date: str, spark) -> None:
    """
    1. Lê raw/customers/ingestion_date={ingestion_date}/*.json.gz
    2. Adiciona colunas de controle
    3. Garante idempotência (delete by _batch_id antes do append)
    4. Faz append na tabela Iceberg lakehouse.bronze.customers
    """
    raw_path = f"s3a://{config.MINIO_BUCKET}/raw/customers/ingestion_date={ingestion_date}/"
    logger.info(f"Lendo raw zone de customers | path: {raw_path}")

    df = spark.read.json(raw_path)

    # Colunas de controle
    _ingested_at = datetime.now(timezone.utc).isoformat()
    _batch_id = ingestion_date  # determinístico — garante idempotência

    df = df.withColumn("_ingested_at", F.lit(_ingested_at).cast("timestamp"))
    df = df.withColumn("_source_file", F.lit(raw_path))
    df = df.withColumn("_batch_id", F.lit(_batch_id))

    # A Bronze declara is_active como STRING (conversão para BOOLEAN é
    # trabalho da Silver). O JSON traz um booleano nativo — cast explícito
    # evita ambiguidade de tipo no momento do append.
    if "is_active" in df.columns:
        df = df.withColumn("is_active", F.col("is_active").cast("string"))

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.BRONZE_NAMESPACE}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.BRONZE_NAMESPACE}.customers (
            customer_id  STRING,
            company_name STRING,
            plan         STRING,
            segment      STRING,
            signup_date  STRING,
            country      STRING,
            is_active    STRING,
            updated_at   STRING,
            _ingested_at TIMESTAMP,
            _source_file STRING,
            _batch_id    STRING
        )
        USING iceberg
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

    # Idempotência: remove o batch anterior antes de reinserir
    spark.sql(f"""
        DELETE FROM {config.BRONZE_NAMESPACE}.customers
        WHERE _batch_id = '{ingestion_date}'
    """)

    # O append do Iceberg exige que a ordem das colunas do DataFrame bata com
    # a ordem declarada na tabela (ver mesmo ajuste em bronze_events.py).
    known_columns = [
        "customer_id", "company_name", "plan", "segment", "signup_date",
        "country", "is_active", "updated_at", "_ingested_at", "_source_file",
        "_batch_id",
    ]
    drift_columns = sorted(c for c in df.columns if c not in known_columns)
    df = df.select(*known_columns, *drift_columns)

    df.writeTo(f"{config.BRONZE_NAMESPACE}.customers").append()

    count = spark.sql(f"""
        SELECT COUNT(*) as total
        FROM {config.BRONZE_NAMESPACE}.customers
        WHERE _batch_id = '{ingestion_date}'
    """).collect()[0]["total"]

    logger.info(
        "bronze_customers_completed",
        extra={
            "ingestion_date": ingestion_date,
            "records_written": count,
            "table": f"{config.BRONZE_NAMESPACE}.customers",
        },
    )


if __name__ == "__main__":
    import argparse
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ingestion_date", required=True,
        help="Data de ingestão no formato YYYY-MM-DD",
    )
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName(f"bronze_customers_{args.ingestion_date}") \
        .getOrCreate()

    try:
        run(args.ingestion_date, spark)
    finally:
        spark.stop()
