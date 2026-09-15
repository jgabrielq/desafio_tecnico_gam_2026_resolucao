import logging
from datetime import datetime, timezone

from pyspark.sql import functions as F

from transform import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("transform.bronze_events")


def run(ingestion_date: str, spark) -> None:
    """
    1. Lê raw/events/ingestion_date={ingestion_date}/*.json.gz
    2. Adiciona colunas de controle
    3. Garante idempotência (delete by _batch_id antes do append)
    4. Faz append na tabela Iceberg lakehouse.bronze.events
    """
    raw_path = f"s3a://{config.MINIO_BUCKET}/raw/events/ingestion_date={ingestion_date}/"
    logger.info(f"Lendo raw zone de events | path: {raw_path}")

    df_raw = spark.read.json(raw_path)

    # Preserva 'properties' como JSON string para tolerar 'schema drift'
    df = df_raw.withColumn("properties", F.to_json(F.col("properties")))

    # Inserindo as colunas de controle
    _ingested_at = datetime.now(timezone.utc).isoformat()
    _batch_id = ingestion_date  # determinístico — garante idempotência

    df = df.withColumn("_ingested_at", F.lit(_ingested_at).cast("timestamp"))
    df = df.withColumn("_source_file", F.lit(raw_path))
    df = df.withColumn("_batch_id", F.lit(_batch_id))

    # Criando o NAMESPACE da camada Bronze no Iceberg
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.BRONZE_NAMESPACE}")

    # Criando a ESTRUTURA da tabela (arquivo .parquet) para armazenar os dados extraídos do JSON
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.BRONZE_NAMESPACE}.events (
            event_id     STRING,
            customer_id  STRING,
            event_type   STRING,
            occurred_at  STRING,
            updated_at   STRING,
            channel      STRING,
            properties   STRING,
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

    # Idempotência: remove os dados do 'batch_id' ('ingestion_date') anterior antes de reinserir
    spark.sql(f"""
        DELETE FROM {config.BRONZE_NAMESPACE}.events
        WHERE _batch_id = '{ingestion_date}'
    """)

    # Salva os dados extraídos do JSON na tabela (.parquet) no Iceberg
    df.writeTo(f"{config.BRONZE_NAMESPACE}.events").append()

    count = spark.sql(f"""
        SELECT COUNT(*) as total
        FROM {config.BRONZE_NAMESPACE}.events
        WHERE _batch_id = '{ingestion_date}'
    """).collect()[0]["total"]

    logger.info(
        "bronze_events_completed",
        extra={
            "ingestion_date": ingestion_date,
            "records_written": count,
            "table": f"{config.BRONZE_NAMESPACE}.events",
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

    with SparkSession.builder \
        .appName(f"bronze_events_{args.ingestion_date}") \
        .getOrCreate() as spark:
        
        run(args.ingestion_date, spark)
