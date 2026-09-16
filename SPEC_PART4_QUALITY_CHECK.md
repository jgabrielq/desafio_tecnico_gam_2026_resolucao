# SPEC — Parte 4: Qualidade e Observabilidade

Especificação técnica completa para implementação dos checks de qualidade.
Leia este arquivo inteiro antes de escrever qualquer código.
Implemente APENAS após Bronze, Silver e Gold estarem validadas.

---

## Contexto e decisões arquiteturais

**Tabela de log persistida**: `lakehouse.quality.check_results` — os resultados
de cada check são gravados no Iceberg, não só impressos no terminal. O enunciado
é explícito: "os resultados devem ser persistidos".

**Dois comportamentos por severidade**:
- `BLOCKING`: lança exceção após persistir o resultado — interrompe o pipeline.
- `WARNING`: persiste o resultado e loga, mas não interrompe.

**Um único script** `quality/checks.py` que roda todos os checks via Spark,
persiste os resultados, e ao final verifica se algum BLOCKING falhou.

**Ordem de execução na DAG**: após Silver, antes de Gold.

---

## Estrutura de arquivos a criar

```
quality/
├── __init__.py
└── checks.py
```

---

## Tabela de log — `lakehouse.quality.check_results`

```sql
CREATE TABLE IF NOT EXISTS lakehouse.quality.check_results (
    check_name       STRING,
    check_type       STRING,
    severity         STRING,
    status           STRING,
    table_name       STRING,
    metric_value     DOUBLE,
    threshold_value  DOUBLE,
    details          STRING,
    batch_id         STRING,
    executed_at      TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('write.format.default' = 'parquet')
```

---

## Função auxiliar de persistência

```python
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
```

---

## Os 5 checks obrigatórios

### Check 1 — Unicidade da chave primária (`event_id`) — BLOCKING

```python
def check_unicidade_event_id(spark, batch_id):
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
```

Por que BLOCKING: duplicatas na chave de negócio indicam falha no MERGE INTO.

---

### Check 2 — Integridade referencial — WARNING

```python
def check_integridade_referencial(spark, batch_id):
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
```

Por que WARNING: o enunciado documenta explicitamente customer_ids órfãos
intencionais — é comportamento conhecido da fonte, não falha do pipeline.

---

### Check 3 — Volumetria fora do esperado — WARNING

```python
def check_volumetria(spark, batch_id):
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
```

---

### Check 4 — Valores de domínio inválidos — WARNING

```python
def check_dominios(spark, batch_id):
    event_types_validos = [
        'login', 'logout', 'page_view', 'purchase', 'ticket_opened',
        'ticket_replied', 'ticket_closed', 'export_generated',
        'report_viewed', 'api_call'
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
```

---

### Check 5 — Freshness — BLOCKING

```python
def check_freshness(spark, batch_id):
    result = spark.sql("""
        SELECT
            MAX(occurred_at) AS max_occurred_at,
            MAX(_ingested_at) AS max_ingested_at,
            DATEDIFF(MAX(_ingested_at), MAX(occurred_at)) AS defasagem_dias
        FROM lakehouse.silver.events
    """).collect()[0]

    defasagem = result["defasagem_dias"] if result["defasagem_dias"] else 0
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
        details=(f"Max occurred_at: {result['max_occurred_at']} | "
                 f"Max ingested_at: {result['max_ingested_at']} | "
                 f"Defasagem: {defasagem} dias"),
        batch_id=batch_id
    )
    return status
```

Por que BLOCKING: dado desatualizado na Silver significa que a Gold calcularia
sobre um snapshot incompleto.

---

## Função orquestradora

```python
def run(ingestion_date: str, spark: SparkSession) -> None:
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
    resultados["unicidade"]   = check_unicidade_event_id(spark, ingestion_date)
    resultados["referencial"] = check_integridade_referencial(spark, ingestion_date)
    resultados["volumetria"]  = check_volumetria(spark, ingestion_date)
    resultados["dominios"]    = check_dominios(spark, ingestion_date)
    resultados["freshness"]   = check_freshness(spark, ingestion_date)

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
```

---

## Ponto de entrada CLI

```python
if __name__ == "__main__":
    import argparse
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion_date", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName(f"quality_checks_{args.ingestion_date}") \
        .getOrCreate()

    try:
        run(args.ingestion_date, spark)
    finally:
        spark.stop()
```

---

## Como executar

```bash
docker cp quality dl-spark:/tmp/quality

docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/quality/checks.py --ingestion_date 2026-03-11
```

---

## Validação após implementação

```bash
# 1. Rodar os checks
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/quality/checks.py --ingestion_date 2026-03-11

# 2. Ver todos os resultados
docker exec dl-trino trino --execute \
    "SELECT check_name, severity, status, metric_value, details
     FROM lakehouse.quality.check_results
     ORDER BY executed_at DESC"

# 3. Filtrar só os que falharam
docker exec dl-trino trino --execute \
    "SELECT * FROM lakehouse.quality.check_results
     WHERE status = 'FAILED'
     ORDER BY executed_at DESC"
```

---

## Como conectar a um alerta real

Via Airflow — `on_failure_callback` na task de qualidade:
```python
def alerta_quality_falhou(context):
    # Buscar resultado do check que falhou em lakehouse.quality.check_results
    # Enviar para Slack/PagerDuty com: check_name, severity, details, batch_id
    pass

quality_task = SparkSubmitOperator(
    task_id="quality_checks",
    on_failure_callback=alerta_quality_falhou,
    ...
)
```

Via monitoramento externo — query periódica no Trino:
```sql
SELECT check_name, severity, status, details, executed_at
FROM lakehouse.quality.check_results
WHERE status = 'FAILED'
  AND executed_at >= NOW() - INTERVAL '24' HOUR
ORDER BY executed_at DESC
```

---

## Notas para o code review

- **Por que persistir e não só logar**: logs são efêmeros. Uma tabela Iceberg
  permite queries históricas — "quantas vezes o check falhou nos últimos 30 dias?"
- **Por que unicidade é BLOCKING e referencial é WARNING**: unicidade quebrada
  indica falha no MERGE INTO (dado corrompido). Referencial quebrada é
  comportamento documentado da fonte (enunciado avisa sobre orphan customer_ids).
- **Por que freshness é BLOCKING**: dado desatualizado na Silver gera análises
  incorretas na Gold — pior que não ter análise.
- **Por que o log não é idempotente**: o histórico de execuções é valioso para
  auditoria. Reprocessar o mesmo batch_id gera múltiplas linhas — isso é
  intencional, não duplicação.