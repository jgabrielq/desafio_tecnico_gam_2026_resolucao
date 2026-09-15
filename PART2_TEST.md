Crie o arquivo ingestion/test_raw_connection.py com o seguinte conteúdo exato:

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test_raw").getOrCreate()

# Teste 1: ler a raw zone
df = spark.read.json("s3a://lakehouse/raw/events/ingestion_date=2026-03-11/")
df.printSchema()
df.show(5)

# Teste 2: catálogo Iceberg
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
spark.sql("SHOW NAMESPACES IN lakehouse").show()

spark.stop()

Não altere nenhum outro arquivo.