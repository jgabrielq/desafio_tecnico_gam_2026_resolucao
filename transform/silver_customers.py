import logging

from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("transform.silver_customers")


def run(ingestion_date: str, spark) -> None:
    """
    1. Lê lakehouse.bronze.customers do batch atual
    2. Aplica SCD Tipo 2 em lakehouse.silver.customers:
       - fecha (valid_to / is_current) as versões substituídas
       - insere as novas versões (clientes novos ou que mudaram de estado)
    """
    df_bronze = spark.sql(f"""
        SELECT *
        FROM lakehouse.bronze.customers
        WHERE _batch_id = '{ingestion_date}'
    """)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.silver.customers (
            customer_id  STRING,
            company_name STRING,
            plan         STRING,
            segment      STRING,
            signup_date  DATE,
            country      STRING,
            is_active    BOOLEAN,
            updated_at   TIMESTAMP,
            valid_from   TIMESTAMP,
            valid_to     TIMESTAMP,
            is_current   BOOLEAN,
            _ingested_at TIMESTAMP,
            _source_file STRING,
            _batch_id    STRING
        )
        USING iceberg
        TBLPROPERTIES (
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

    # Passo 1 — tipagem do batch de entrada
    df_new = df_bronze \
        .withColumn("updated_at",  F.to_timestamp("updated_at")) \
        .withColumn("signup_date", F.to_date("signup_date")) \
        .withColumn("is_active",   F.col("is_active").cast("boolean")) \
        .withColumn("valid_from",  F.col("updated_at")) \
        .withColumn("valid_to",    F.lit("9999-12-31T23:59:59").cast("timestamp")) \
        .withColumn("is_current",  F.lit(True))

    df_new.createOrReplaceTempView("customers_batch")

    # Passo 3 — fecha as versões antigas que foram substituídas
    spark.sql("""
        MERGE INTO lakehouse.silver.customers AS target
        USING customers_batch AS source
        ON target.customer_id = source.customer_id
           AND target.is_current = true
           AND source.updated_at > target.updated_at
        WHEN MATCHED
            THEN UPDATE SET
                target.valid_to   = source.updated_at,
                target.is_current = false
    """)

    # Passo 4 — insere as novas versões (clientes novos ou que mudaram de estado)
    spark.sql("""
        MERGE INTO lakehouse.silver.customers AS target
        USING customers_batch AS source
        ON target.customer_id = source.customer_id
           AND target.valid_from = source.valid_from
        WHEN NOT MATCHED
            THEN INSERT *
    """)

    current_count = spark.sql("""
        SELECT COUNT(*) as total
        FROM lakehouse.silver.customers
        WHERE is_current = true
    """).collect()[0]["total"]

    history_count = spark.sql("""
        SELECT COUNT(*) as total
        FROM lakehouse.silver.customers
    """).collect()[0]["total"]

    logger.info(
        "silver_customers_completed",
        extra={
            "ingestion_date": ingestion_date,
            "current_records": current_count,
            "total_history_records": history_count,
            "table": "lakehouse.silver.customers",
        },
    )


if __name__ == "__main__":
    import argparse
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion_date", required=True)
    args = parser.parse_args()

    with SparkSession.builder \
        .appName(f"silver_customers_{args.ingestion_date}") \
        .getOrCreate() as spark:

        run(args.ingestion_date, spark)
