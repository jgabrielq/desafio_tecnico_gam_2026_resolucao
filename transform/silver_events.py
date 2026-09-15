import logging

from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("transform.silver_events")


def run(ingestion_date: str, spark) -> None:
    """
    1. Lê lakehouse.bronze.events do batch atual
    2. Deduplica por event_id mantendo o maior updated_at
    3. Normaliza tipos e timestamps
    4. Faz MERGE INTO em lakehouse.silver.events
    """
    df_bronze = spark.sql(f"""
        SELECT *
        FROM lakehouse.bronze.events
        WHERE _batch_id = '{ingestion_date}'
    """)

    # Deduplicação local: mantém apenas o registro com maior updated_at por event_id
    window = Window.partitionBy("event_id").orderBy(F.col("updated_at").desc())

    df_deduped = df_bronze \
        .withColumn("_rank", F.row_number().over(window)) \
        .filter(F.col("_rank") == 1) \
        .drop("_rank")

    df_silver = df_deduped \
        .withColumn("occurred_at",
                    F.to_timestamp(F.col("occurred_at"))) \
        .withColumn("updated_at",
                    F.to_timestamp(F.col("updated_at"))) \
        .withColumn("_customer_exists", F.col("customer_id").isNotNull()) \
        .select(
            "event_id",
            "customer_id",
            "event_type",
            "occurred_at",
            "updated_at",
            "channel",
            "properties",        # já é STRING (convertido na Bronze)
            "_customer_exists",  # flag para eventos órfãos
            "_ingested_at",
            "_source_file",
            "_batch_id",
        )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.events (
            event_id          STRING,
            customer_id       STRING,
            event_type        STRING,
            occurred_at       TIMESTAMP,
            updated_at        TIMESTAMP,
            channel           STRING,
            properties        STRING,
            _customer_exists  BOOLEAN,
            _ingested_at      TIMESTAMP,
            _source_file      STRING,
            _batch_id         STRING
        )
        USING iceberg
        PARTITIONED BY (days(occurred_at))
        TBLPROPERTIES (
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.merge.mode'                = 'merge-on-read'
        )
    """)

    df_silver.createOrReplaceTempView("silver_events_batch")

    # MERGE INTO: upsert idempotente — registros mais recentes sobrescrevem, mais antigos são ignorados
    spark.sql("""
        MERGE INTO lakehouse.silver.events AS target
        USING silver_events_batch AS source
        ON target.event_id = source.event_id
        WHEN MATCHED AND source.updated_at > target.updated_at
            THEN UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

    count = spark.sql("""
        SELECT COUNT(*) as total FROM lakehouse.silver.events
    """).collect()[0]["total"]

    logger.info(
        "silver_events_completed",
        extra={
            "ingestion_date": ingestion_date,
            "total_records_silver": count,
            "table": "lakehouse.silver.events",
        },
    )


if __name__ == "__main__":
    import argparse
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion_date", required=True)
    args = parser.parse_args()

    with SparkSession.builder \
        .appName(f"silver_events_{args.ingestion_date}") \
        .getOrCreate() as spark:

        run(args.ingestion_date, spark)
