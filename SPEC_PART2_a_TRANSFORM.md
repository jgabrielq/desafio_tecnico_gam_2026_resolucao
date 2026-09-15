# SPEC — Parte 2a: Bronze (PySpark + Iceberg)

Especificação técnica completa para implementação da camada Bronze.
Leia este arquivo inteiro antes de escrever qualquer código.

---

## Contexto

A camada Bronze é a primeira camada do lakehouse Iceberg. Ela lê os arquivos
brutos da raw zone (NDJSON gzip, particionados por `ingestion_date`) e os
persiste em tabelas Iceberg com tipagem básica e colunas de controle.

**Princípio central**: Bronze é fiel à raw. Nenhuma transformação de negócio
acontece aqui — apenas tipagem, adição de colunas de controle, e persistência
em formato Iceberg. Toda limpeza e deduplicação ficam para a Silver.

---

## Estrutura de arquivos a criar

```
transform/
├── __init__.py
├── config.py          # configurações Spark + Iceberg (reutilizar padrões do ambiente)
├── bronze_events.py   # job PySpark: raw events → lakehouse.bronze.events
└── bronze_customers.py # job PySpark: raw customers → lakehouse.bronze.customers
```

---

## `transform/config.py`

Configurações compartilhadas entre todos os jobs PySpark. O ambiente já
configura o Spark via `spark-defaults.conf` — este arquivo só expõe as
constantes de negócio.

```python
import os
from dotenv import load_dotenv

load_dotenv()

# MinIO
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",      "lakehouse")

# Catálogo Iceberg (já configurado no ambiente)
ICEBERG_CATALOG  = "lakehouse"

# Namespaces
BRONZE_NAMESPACE = f"{ICEBERG_CATALOG}.bronze"
SILVER_NAMESPACE = f"{ICEBERG_CATALOG}.silver"

# Prefixos raw zone
RAW_EVENTS_PREFIX    = "raw/events"
RAW_CUSTOMERS_PREFIX = "raw/customers"
```

**ATENÇÃO**: dentro dos containers, o MinIO é acessado via `http://minio:9000`,
não `http://localhost:9000`. O `spark-defaults.conf` do ambiente já configura
isso — não sobrescrever essas configurações no código.

---

## `bronze_events.py`

### Responsabilidade
Ler os arquivos NDJSON gzip da raw zone de events e persistir na tabela
Iceberg `lakehouse.bronze.events` como append-only.

### Ponto de entrada
```bash
spark-submit transform/bronze_events.py --ingestion_date 2026-03-11
```

### Função principal

```python
def run(ingestion_date: str, spark: SparkSession) -> None:
    """
    1. Lê raw/events/ingestion_date={ingestion_date}/*.json.gz
    2. Adiciona colunas de controle
    3. Garante idempotência (delete by _batch_id antes do append)
    4. Faz append na tabela Iceberg lakehouse.bronze.events
    """
```

### Leitura da raw zone

```python
raw_path = f"s3a://{MINIO_BUCKET}/raw/events/ingestion_date={ingestion_date}/"
df_raw = spark.read.json(raw_path)
```

O Spark vai inferir o schema automaticamente a partir do JSON. Isso é
intencional na Bronze — não forçar schema fixo permite absorver schema drift
(campo novo na API) sem quebrar.

### Tratamento do campo `properties`

O campo `properties` chegou como struct aninhado na leitura do teste anterior.
Na Bronze, converter para string JSON para preservar o payload exatamente
como veio e evitar perda de campos com schema drift:

```python
from pyspark.sql import functions as F

df = df_raw.withColumn("properties", F.to_json(F.col("properties")))
```

### Colunas de controle a adicionar

```python
from datetime import datetime, timezone

_ingested_at = datetime.now(timezone.utc).isoformat()
_batch_id    = ingestion_date  # determinístico — garante idempotência

df = df.withColumn("_ingested_at", F.lit(_ingested_at).cast("timestamp"))
df = df.withColumn("_source_file", F.lit(raw_path))
df = df.withColumn("_batch_id",    F.lit(_batch_id))
```

### Criação da tabela Iceberg (se não existir)

```python
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {BRONZE_NAMESPACE}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_NAMESPACE}.events (
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
```

**Por que sem particionamento na Bronze**: Bronze é append-only e fiel à raw.
O particionamento por `ingestion_date` já está na raw zone — na Bronze, o
particionamento vai acontecer na Silver (por `occurred_at`), onde os dados
já estarão deduplicados e com tipos corretos.

### Idempotência — DELETE por `_batch_id` antes do append

```python
spark.sql(f"""
    DELETE FROM {BRONZE_NAMESPACE}.events
    WHERE _batch_id = '{ingestion_date}'
""")
```

Rodar o job duas vezes para o mesmo `ingestion_date` remove os registros
anteriores e reinsere — resultado idêntico. Iceberg suporta DELETE eficiente
(não reescreve o arquivo inteiro, usa delete files).

### Append na tabela Iceberg

```python
df.writeTo(f"{BRONZE_NAMESPACE}.events").append()
```

### Log ao final

```python
count = spark.sql(f"""
    SELECT COUNT(*) as total
    FROM {BRONZE_NAMESPACE}.events
    WHERE _batch_id = '{ingestion_date}'
""").collect()[0]["total"]

logger.info("bronze_events_completed", extra={
    "ingestion_date": ingestion_date,
    "records_written": count,
    "table": f"{BRONZE_NAMESPACE}.events"
})
```

### Ponto de entrada CLI

```python
if __name__ == "__main__":
    import argparse
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion_date", required=True,
                        help="Data de ingestão no formato YYYY-MM-DD")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName(f"bronze_events_{args.ingestion_date}") \
        .getOrCreate()

    try:
        run(args.ingestion_date, spark)
    finally:
        spark.stop()
```

---

## `bronze_customers.py`

### Responsabilidade
Mesma lógica do `bronze_events.py`, adaptada para a fonte customers.

### Diferenças em relação ao bronze_events

**Leitura**:
```python
raw_path = f"s3a://{MINIO_BUCKET}/raw/customers/ingestion_date={ingestion_date}/"
df_raw = spark.read.json(raw_path)
```

**Sem conversão de `properties`**: customers não tem campo `properties`.

**Schema da tabela Iceberg**:
```python
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_NAMESPACE}.customers (
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
```

**Por que `is_active` como STRING na Bronze**: Bronze preserva os dados como
vieram. A conversão para BOOLEAN acontece na Silver, onde temos controle total
do schema.

**Idempotência**:
```python
spark.sql(f"""
    DELETE FROM {BRONZE_NAMESPACE}.customers
    WHERE _batch_id = '{ingestion_date}'
""")
```

**Append**:
```python
df.writeTo(f"{BRONZE_NAMESPACE}.customers").append()
```

**Ponto de entrada CLI**:
```bash
spark-submit transform/bronze_customers.py --ingestion_date 2026-03-11
```

---

## Como executar os jobs

Os jobs rodam dentro do container Spark, que já tem acesso ao catálogo
Iceberg e ao MinIO configurados:

```bash
# Bronze events
docker compose exec spark spark-submit \
    /home/iceberg/work/transform/bronze_events.py \
    --ingestion_date 2026-03-11

# Bronze customers
docker compose exec spark spark-submit \
    /home/iceberg/work/transform/bronze_customers.py \
    --ingestion_date 2026-03-11
```

---

## Validação após implementação

```bash
# 1. Rodar os dois jobs
docker compose exec spark spark-submit \
    /home/iceberg/work/transform/bronze_events.py --ingestion_date 2026-03-11

docker compose exec spark spark-submit \
    /home/iceberg/work/transform/bronze_customers.py --ingestion_date 2026-03-11

# 2. Validar via Trino (os dados aparecem imediatamente)
docker compose exec trino trino --execute \
    "SELECT COUNT(*), _batch_id FROM lakehouse.bronze.events GROUP BY _batch_id"

docker compose exec trino trino --execute \
    "SELECT COUNT(*), _batch_id FROM lakehouse.bronze.customers GROUP BY _batch_id"

# 3. Teste de idempotência — rodar de novo e confirmar mesma contagem
docker compose exec spark spark-submit \
    /home/iceberg/work/transform/bronze_events.py --ingestion_date 2026-03-11

# Contagem deve ser igual à do passo 2
```

---

## Notas para o code review

- O avaliador vai perguntar por que `properties` foi convertido para STRING na Bronze.
  Resposta: preservar o payload exatamente como veio + tolerar schema drift sem quebrar.
- O avaliador vai perguntar por que Bronze não tem particionamento por data.
  Resposta: particionamento acontece na Silver, onde os dados estão deduplicados.
  Bronze é append-only e o volume por `_batch_id` é pequeno.
- O avaliador vai testar idempotência rodando o job duas vezes. O DELETE por
  `_batch_id` antes do append garante resultado idêntico.
- `_batch_id = ingestion_date` é determinístico — não usar timestamp nem UUID,
  que quebrariam a idempotência.