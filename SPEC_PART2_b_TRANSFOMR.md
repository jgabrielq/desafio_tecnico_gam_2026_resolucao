# SPEC — Parte 2b: Silver (PySpark + Iceberg)

Especificação técnica completa para implementação da camada Silver.
Leia este arquivo inteiro antes de escrever qualquer código.
Implemente APENAS após a Bronze estar validada.

---

## Contexto

A camada Silver é a camada curada do lakehouse. Ela lê da Bronze, aplica
deduplicação via `MERGE INTO`, normaliza tipos e timestamps, e modela o
histórico de mudanças dos clientes (SCD Tipo 2).

**Princípio central**: Silver é a fonte de verdade para consumo analítico.
Toda deduplicação, tipagem correta e modelagem dimensional acontece aqui.

> **Nota de implementação**: este spec foi implementado literalmente, sem
> nenhuma correção funcional — a lógica de dedup, os dois `MERGE INTO` do
> SCD2 e o particionamento funcionaram como descritos, inclusive validados
> contra o batch 2 (schema drift e mudanças de plano). A única diferença em
> relação ao código mostrado abaixo é estilística, não uma correção: nos
> blocos `if __name__ == "__main__":` de `silver_events.py` e
> `silver_customers.py`, o `try/finally` foi trocado por
> `with SparkSession.builder...as spark:` — por consistência com o estilo já
> adotado em `bronze_events.py`, sem impacto de comportamento (`SparkSession`
> suporta o protocolo de context manager desde o Spark 3.2 e chama
> `spark.stop()` na saída do bloco da mesma forma). Os ajustes reais feitos
> durante a Parte 2 (schema drift, cast de tipos, boundary de watermark)
> aconteceram todos na Bronze e na Ingestão, não na Silver — ver
> `AJUSTES_PARTE2_TRANSFORM.md`.

---

## Estrutura de arquivos a criar

```
transform/
├── silver_events.py    # job PySpark: bronze.events → silver.events
└── silver_customers.py # job PySpark: bronze.customers → silver.customers (SCD2)
```

---

## `silver_events.py`

### Responsabilidade
Ler `lakehouse.bronze.events`, deduplicar por `event_id` mantendo o registro
com maior `updated_at`, normalizar tipos e timestamps, e persistir em
`lakehouse.silver.events` via `MERGE INTO`.

### Ponto de entrada
```bash
spark-submit transform/silver_events.py --ingestion_date 2026-03-11
```

### Leitura da Bronze — apenas o batch atual

```python
df_bronze = spark.sql(f"""
    SELECT *
    FROM lakehouse.bronze.events
    WHERE _batch_id = '{ingestion_date}'
""")
```

### Deduplicação local (antes do MERGE)

A Bronze pode conter duplicatas de `event_id` (a API envia duplicatas entre
páginas). Antes do MERGE, manter apenas o registro com maior `updated_at`
por `event_id`:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window = Window.partitionBy("event_id").orderBy(F.col("updated_at").desc())

df_deduped = df_bronze \
    .withColumn("_rank", F.row_number().over(window)) \
    .filter(F.col("_rank") == 1) \
    .drop("_rank")
```

### Tipagem e normalização

```python
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
        "_batch_id"
    )
```

### Criação da tabela Silver (se não existir)

```python
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
```

**Particionamento por `days(occurred_at)`**:
- As 3 queries da Gold filtram por janela temporal (últimos 30 dias, por mês,
  por coorte mensal) — partition pruning reduz drasticamente o volume lido.
- Por que NÃO por `event_type`: baixa cardinalidade + partições desbalanceadas.
- Por que NÃO por `customer_id`: altíssima cardinalidade → problema grave de
  small files.
- `'write.merge.mode' = 'merge-on-read'`: otimiza o MERGE INTO para escrita
  rápida (acumula deletes/updates em arquivos separados, em vez de reescrever
  as partições inteiras a cada MERGE).

### Registro temporário para o MERGE

```python
df_silver.createOrReplaceTempView("silver_events_batch")
```

### MERGE INTO — deduplicação e upsert

```python
spark.sql("""
    MERGE INTO lakehouse.silver.events AS target
    USING silver_events_batch AS source
    ON target.event_id = source.event_id
    WHEN MATCHED AND source.updated_at > target.updated_at
        THEN UPDATE SET *
    WHEN NOT MATCHED
        THEN INSERT *
""")
```

**Por que `WHEN MATCHED AND source.updated_at > target.updated_at`**:
Garante que registros corrigidos (mesmo `event_id` com `updated_at` mais
recente) sobrescrevem a versão anterior. Registros mais antigos que o que já
está na Silver são ignorados — idempotência perfeita.

### Log ao final

```python
count = spark.sql("""
    SELECT COUNT(*) as total FROM lakehouse.silver.events
""").collect()[0]["total"]

logger.info("silver_events_completed", extra={
    "ingestion_date": ingestion_date,
    "total_records_silver": count,
    "table": "lakehouse.silver.events"
})
```

### Ponto de entrada CLI

```python
if __name__ == "__main__":
    import argparse
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion_date", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName(f"silver_events_{args.ingestion_date}") \
        .getOrCreate()

    try:
        run(args.ingestion_date, spark)
    finally:
        spark.stop()
```

---

## `silver_customers.py` — SCD Tipo 2

### Responsabilidade
Ler `lakehouse.bronze.customers`, aplicar SCD Tipo 2 em
`lakehouse.silver.customers`, permitindo responder "qual era o plano do
cliente na data do evento".

### O que é SCD Tipo 2 e por que aqui

Entre o batch 1 e o batch 2, alguns clientes mudam de plano. Se sobrescrevêssemos
o registro (SCD Tipo 1), perderíamos o histórico — não saberíamos mais qual era
o plano do cliente no momento de um evento antigo.

Com SCD Tipo 2, cada mudança de estado gera uma nova linha com:
- `valid_from`: quando essa versão passou a valer (`updated_at` do registro)
- `valid_to`: quando essa versão foi substituída (ou `9999-12-31` se for a atual)
- `is_current`: `true` para a versão vigente

Para saber o plano de um cliente na data de um evento:
```sql
SELECT c.plan
FROM silver.events e
JOIN silver.customers c
  ON e.customer_id = c.customer_id
 AND e.occurred_at BETWEEN c.valid_from AND c.valid_to
```

### Leitura da Bronze — apenas o batch atual

```python
df_bronze = spark.sql(f"""
    SELECT *
    FROM lakehouse.bronze.customers
    WHERE _batch_id = '{ingestion_date}'
""")
```

### Criação da tabela Silver (se não existir)

```python
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
```

**Sem particionamento**: tabela de dimensão pequena. Particionar uma dimensão
de poucos milhares de linhas cria overhead sem benefício de pruning.

### Lógica SCD Tipo 2 — passo a passo

**Passo 1 — Tipagem do batch de entrada**:
```python
df_new = df_bronze \
    .withColumn("updated_at",  F.to_timestamp("updated_at")) \
    .withColumn("signup_date", F.to_date("signup_date")) \
    .withColumn("is_active",   F.col("is_active").cast("boolean")) \
    .withColumn("valid_from",  F.col("updated_at")) \
    .withColumn("valid_to",    F.lit("9999-12-31T23:59:59").cast("timestamp")) \
    .withColumn("is_current",  F.lit(True))
```

**Passo 2 — Identificar registros que mudaram**:

Registros que já existem na Silver com `is_current = true` e cujo `updated_at`
novo é maior que o `updated_at` atual:

```python
df_new.createOrReplaceTempView("customers_batch")

df_changed = spark.sql("""
    SELECT n.customer_id
    FROM customers_batch n
    JOIN lakehouse.silver.customers c
      ON n.customer_id = c.customer_id
     AND c.is_current = true
     AND n.updated_at > c.updated_at
""")
```

**Passo 3 — Fechar versões antigas (atualizar `valid_to` e `is_current`)**:

```python
spark.sql(f"""
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
```

**Passo 4 — Inserir novas versões**:

Inserir apenas os registros que de fato mudaram (ou que são novos):

```python
spark.sql("""
    MERGE INTO lakehouse.silver.customers AS target
    USING customers_batch AS source
    ON target.customer_id = source.customer_id
       AND target.valid_from = source.valid_from
    WHEN NOT MATCHED
        THEN INSERT *
""")
```

**Por que dois MERGEs separados**: Iceberg não suporta `WHEN MATCHED ... UPDATE`
e `WHEN NOT MATCHED ... INSERT` na mesma instrução quando a lógica de matching
é diferente para cada caso. Separar garante clareza e corretude.

### Log ao final

```python
current_count = spark.sql("""
    SELECT COUNT(*) as total
    FROM lakehouse.silver.customers
    WHERE is_current = true
""").collect()[0]["total"]

history_count = spark.sql("""
    SELECT COUNT(*) as total
    FROM lakehouse.silver.customers
""").collect()[0]["total"]

logger.info("silver_customers_completed", extra={
    "ingestion_date": ingestion_date,
    "current_records": current_count,
    "total_history_records": history_count,
    "table": "lakehouse.silver.customers"
})
```

---

## Como executar os jobs

```bash
# Silver events
docker compose exec spark spark-submit \
    /home/iceberg/work/transform/silver_events.py \
    --ingestion_date 2026-03-11

# Silver customers
docker compose exec spark spark-submit \
    /home/iceberg/work/transform/silver_customers.py \
    --ingestion_date 2026-03-11
```

---

## Validação após implementação

```bash
# 1. Rodar os jobs
docker compose exec spark spark-submit \
    /home/iceberg/work/transform/silver_events.py --ingestion_date 2026-03-11

docker compose exec spark spark-submit \
    /home/iceberg/work/transform/silver_customers.py --ingestion_date 2026-03-11

# 2. Validar deduplicação (silver deve ter <= bronze)
docker compose exec trino trino --execute \
    "SELECT COUNT(*) FROM lakehouse.silver.events"

docker compose exec trino trino --execute \
    "SELECT COUNT(*) FROM lakehouse.bronze.events WHERE _batch_id = '2026-03-11'"

# 3. Validar SCD2 — todos os clientes atuais
docker compose exec trino trino --execute \
    "SELECT COUNT(*) FROM lakehouse.silver.customers WHERE is_current = true"

# 4. Validar particionamento de events
docker compose exec trino trino --execute \
    "SELECT DISTINCT day(occurred_at) FROM lakehouse.silver.events ORDER BY 1"

# 5. Teste de idempotência — rodar silver_events de novo
docker compose exec spark spark-submit \
    /home/iceberg/work/transform/silver_events.py --ingestion_date 2026-03-11
# Contagem deve ser IGUAL à do passo 2 — MERGE não duplica
```

---

## Notas para o code review

- **Por que MERGE INTO e não overwrite**: overwrite reescreveria todas as
  partições, perdendo dados de datas anteriores. MERGE atualiza cirurgicamente
  só o que mudou — idempotência sem perda de histórico.
- **Por que dois MERGEs no SCD2**: separar o "fechar versão antiga" do
  "inserir versão nova" evita ambiguidade na condição de matching.
- **Por que `valid_to = 9999-12-31`**: valor sentinela padrão da indústria
  para "sem data de expiração definida" — facilita queries com BETWEEN sem
  tratar NULL.
- **Por que `_customer_exists` em events**: eventos com `customer_id` nulo ou
  inexistente são preservados na Silver com flag, nunca descartados. Dados
  não devem ser apagados silenciosamente — a Gold e os checks de qualidade
  decidem o que fazer com eles.
- **Critério duro do desafio**: rodar duas vezes deve produzir o mesmo
  resultado. O MERGE INTO garante isso — registros já existentes não são
  duplicados, apenas atualizados se `updated_at` for maior.