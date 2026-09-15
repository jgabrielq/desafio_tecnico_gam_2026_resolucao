# CLAUDE.md

Guia de contexto para retomar este repositório em sessões futuras do Claude Code.

## Sobre o projeto

Desafio técnico de engenharia de dados: pipeline de lakehouse on-premises
(MinIO · Iceberg · Spark · Trino · Airflow), com ingestão raw → bronze → silver → gold,
checks de qualidade e um DAG de orquestração. Prazo de entrega: **quarta-feira 16/09 às 16h**.

Documentos de referência (já existentes, não recriar):
- `docs/plano_desafio_tecnico.md` — cronograma e decisões de arquitetura para todas as partes.
- `docs/ARQUITETURA.md`, `docs/DISCUSSOES_PARTE1.md`, `docs/ENVIRONMENT_FIXES.md`, `docs/README_ambiente.md`.
- `SPEC_PART1_INGESTION.md` — especificação técnica da Parte 1 (ingestão).
- `PART2_TEST.md` — instrução pontual para o teste de leitura da raw zone via Spark/Iceberg.
- `README.md` — documentação do módulo `ingestion/`.

## Ambiente de infraestrutura

**Não sobe infra a partir deste repositório.** Os serviços já existem via `docker-compose`
de outro repositório e ficam de pé entre sessões. Containers observados:

| Container | Imagem | Portas | Papel |
|---|---|---|---|
| `dl-minio` | `minio/minio` | 9000/9001 | Object storage (raw zone, warehouse Iceberg) |
| `dl-mock-api` | `desafio-lakehouse-mock-api` | 8000 | API REST mock (fonte `events`) |
| `dl-postgres` | `postgres:16` | 5432 | CRM Postgres (fonte `customers`, schema `crm`) |
| `dl-iceberg-rest` | `tabulario/iceberg-rest` | 8181 | Catálogo REST do Iceberg (`lakehouse`) |
| `dl-spark` | `desafio-lakehouse/spark-iceberg:local` | 8888/10000/4041 | Spark 3.5.5 + Iceberg já configurado |
| `dl-trino` | `trinodb/trino:450` | 8080 | Query engine para a camada gold |

Antes de assumir que a infra está fora do ar, checar com `docker ps`. As credenciais/portas
usadas pelo código Python ficam em `ingestion/config.py` (fallback local: MinIO `admin`/`minioadmin`,
bucket `lakehouse`; Postgres `app`/`app`, db `crm`).

## Build / instalação

Dependências Python da ingestão (não há build compilado):

```bash
pip install -r requirements.txt
# ou, com Pipenv:
pipenv install
```

`requirements.txt` / `Pipfile`: `boto3`, `requests`, `psycopg2-binary`, `python-dotenv`, `tenacity`.
`pyspark` **não** está instalado localmente — jobs Spark rodam dentro do container `dl-spark`,
que já tem Spark 3.5.5 + Iceberg + hadoop-aws configurados em `/opt/spark/conf/spark-defaults.conf`.

## Como rodar

Pipeline de ingestão (raw zone), incremental via watermark:

```bash
python -m ingestion.ingest [YYYY-MM-DD]   # usa a data de hoje se omitida
```

Scripts de teste manual em `tests/` (não são suíte pytest formal, cada um tem `run_test()`
e roda isolado):

```bash
python tests/test_storage.py
python tests/test_watermark.py
python tests/test_api_client.py
python tests/test_postgres_client.py
```

Teste de leitura da raw zone + catálogo Iceberg via Spark — **roda dentro do container**,
não localmente (não há PySpark instalado no host):

```bash
docker cp ingestion/test_raw_connection.py dl-spark:/tmp/test_raw_connection.py
docker exec dl-spark spark-submit /tmp/test_raw_connection.py
```

## Estado atual do trabalho (última sessão)

**Parte 1 (Ingestão / raw zone): concluída e validada.**

- `ingestion/` completo: `config.py`, `api_client.py` (paginação + retry 429/5xx),
  `postgres_client.py`, `watermark.py`, `storage.py` (NDJSON + gzip, particionamento Hive
  por `ingestion_date`), `ingest.py` (orquestrador).
- Correções cirúrgicas descritas em `INGESTIONS_FIXES.md` (compressão gzip, `ingestion_date`
  em vez de `batch_id`) já aplicadas em `ingest.py` e `storage.py`.
- Ingestão já foi executada com sucesso para `ingestion_date=2026-03-11` — dados confirmados
  no MinIO em `raw/events/...`, `raw/customers/...` e `raw/control/watermarks.json`.
- Teste de conexão Spark → raw zone + catálogo Iceberg (`ingestion/test_raw_connection.py`,
  conteúdo definido por `PART2_TEST.md`) **executado dentro do container `dl-spark`** com sucesso:
  - Leitura de `s3a://lakehouse/raw/events/ingestion_date=2026-03-11/` funcionando, schema e
    dados corretos (incluindo o `properties` semiestruturado).
  - `CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze` e `SHOW NAMESPACES IN lakehouse`
    confirmam que o namespace `bronze` já existe no catálogo REST do Iceberg.

### Próximos passos (antes da entrega de quarta 16/09)

Conforme `docs/plano_desafio_tecnico.md`:

1. **Parte 2 — Bronze + Silver**: jobs PySpark para materializar tabelas Iceberg em
   `lakehouse.bronze` (a partir da raw zone) e `lakehouse.silver` (dedup, `MERGE INTO`,
   SCD2 em `silver.customers` para histórico de plano/segmento na data do evento).
2. **Parte 3 — Gold + Trino**: tabelas gold e as 3 queries analíticas exigidas pelo
   enunciado, com resultados em CSV e interpretação escrita.
3. **Parte 4 — Qualidade**: 5+ checks de qualidade com severidade, persistidos em
   tabela Iceberg (não apenas logados).
4. **Parte 5 — Orquestração + Docs**: DAG do Airflow encadeando raw → bronze → silver →
   gold → checks; finalizar `README.md` e `ARCHITECTURE.md`.
5. Antes da entrega: validar idempotência rodando a pipeline duas vezes para o mesmo
   `ingestion_date` (a raw não pode duplicar nem corromper dados — decisão registrada no
   plano é overwrite da partição do dia).

Pontos de decisão já tomados (não reabrir sem motivo, ver `docs/plano_desafio_tecnico.md`):
watermark incremental em arquivo JSON no MinIO (não em tabela Iceberg nem Airflow Variable);
SCD2 completo na `silver.customers`; eventos com `customer_id` órfão são mantidos com flag,
nunca descartados silenciosamente.
