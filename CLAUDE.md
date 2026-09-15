# CLAUDE.md

Guia de contexto para retomar este repositório em sessões futuras do Claude Code.

## Sobre o projeto

Desafio técnico de engenharia de dados: pipeline de lakehouse on-premises
(MinIO · Iceberg · Spark · Trino · Airflow), com ingestão raw → bronze → silver → gold,
checks de qualidade e um DAG de orquestração. Prazo de entrega: **quarta-feira 16/09 às 16h**.

Documentos de referência (já existentes, não recriar):
- `docs/plano_desafio_tecnico.md` — cronograma e decisões de arquitetura para todas as partes.
- `docs/ARQUITETURA.md`, `docs/DISCUSSOES_PARTE1.md`, `docs/DISCUSSOES_PART2_a.md`, `docs/PRIMER_ICEBERG.md`, `docs/ENVIRONMENT_FIXES.md`, `docs/README_ambiente.md`.
- `SPEC_PART1_INGESTION.md` — especificação técnica da Parte 1 (ingestão).
- `PART2_TEST.md` — instrução pontual para o teste de leitura da raw zone via Spark/Iceberg.
- `SPEC_PART2_a_TRANSFORM.md` / `SPEC_PART2_b_TRANSFOMR.md` — especificações técnicas da Bronze e da Silver.
- `AJUSTES_PARTE2_TRANSFORM.md` — problemas descobertos ao validar a Parte 2 de ponta a ponta (schema drift, cast de tipos, boundary do watermark), com causa raiz e correção de cada um. Ler antes de mexer em `transform/` ou no `api_client.py`.
- `README.md` — documentação de `ingestion/` e `transform/` (inclui diagramas Mermaid de execução e arquitetura).

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

Dependências Python da ingestão (não há build compilado). Usar **Pipenv** — o Python do
sistema é "externally managed" e recusa `pip install` direto:

```bash
pipenv install
```

`requirements.txt` / `Pipfile`: `boto3`, `requests`, `psycopg2-binary`, `python-dotenv`, `tenacity`.
`pyspark` **não** está instalado localmente — jobs Spark rodam dentro do container `dl-spark`,
que já tem Spark 3.5.5 + Iceberg + hadoop-aws configurados em `/opt/spark/conf/spark-defaults.conf`.

## Como rodar

Pipeline de ingestão (raw zone), incremental via watermark, rodando no host:

```bash
pipenv run python -m ingestion.ingest [YYYY-MM-DD]   # usa a data de hoje se omitida
```

Scripts de teste manual em `tests/` (não são suíte pytest formal, cada um tem `run_test()`
e roda isolado):

```bash
pipenv run python tests/test_storage.py
pipenv run python tests/test_watermark.py
pipenv run python tests/test_api_client.py
pipenv run python tests/test_postgres_client.py
```

Teste de leitura da raw zone + catálogo Iceberg via Spark — **roda dentro do container**,
não localmente (não há PySpark instalado no host):

```bash
docker cp ingestion/test_raw_connection.py dl-spark:/tmp/test_raw_connection.py
docker exec dl-spark spark-submit /tmp/test_raw_connection.py
```

Jobs de transformação (`transform/`) — Bronze e Silver — também rodam dentro do
container Spark. Copiar a pasta inteira (não só um arquivo) e usar `PYTHONPATH=/tmp`
para o import `from transform import config` resolver:

```bash
docker cp transform dl-spark:/tmp/transform

docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/bronze_events.py --ingestion_date YYYY-MM-DD
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/bronze_customers.py --ingestion_date YYYY-MM-DD
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/silver_events.py --ingestion_date YYYY-MM-DD
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/silver_customers.py --ingestion_date YYYY-MM-DD
```

Validar resultados via Trino:

```bash
docker exec dl-trino trino --execute "SELECT COUNT(*), _batch_id FROM lakehouse.bronze.events GROUP BY _batch_id"
docker exec dl-trino trino --execute "SELECT COUNT(*) FROM lakehouse.silver.customers WHERE is_current = true"
```

Para avançar as fontes para o batch 2 (simula o tempo passando) e resetar o ambiente
do zero quando precisar de um estado limpo (destrutivo — apaga volumes Docker):

```bash
cd /home/jgabrielq/repo_desafio_tecnico/desafio-pleno-2026-2   # repo de infra
make batch2      # libera batch 2 na API + aplica mudanças no Postgres (irreversível sem make clean)
make clean        # derruba containers e apaga volumes (MinIO, Postgres, Iceberg)
make up           # sobe tudo de novo e valida (12/12 checks)
```

## Estado atual do trabalho (última sessão)

**Parte 1 (Ingestão / raw zone): concluída e validada.**
**Parte 2 (Bronze + Silver): concluída e validada de ponta a ponta, incluindo batch 2.**

Repositório git inicializado nesta sessão (`git init` + commits incrementais); antes
disso o diretório não era um repo.

- `ingestion/` completo: `config.py`, `api_client.py` (paginação + retry 429/5xx +
  filtro client-side do boundary do watermark — ver abaixo), `postgres_client.py`,
  `watermark.py`, `storage.py` (NDJSON + gzip, particionamento Hive por `ingestion_date`),
  `ingest.py` (orquestrador).
- `transform/` completo: `config.py`, `bronze_events.py`, `bronze_customers.py`,
  `silver_events.py`, `silver_customers.py` (SCD Tipo 2).
- Teste de conexão Spark → raw zone + catálogo Iceberg (`ingestion/test_raw_connection.py`,
  `PART2_TEST.md`) validado dentro do container `dl-spark`.
- **Sequência de teste completa do desafio executada e validada** (ambiente resetado do
  zero com `make clean && make up` antes do teste definitivo, para garantir estado limpo):
  `pipeline (batch1) → pipeline (batch1 de novo) → make batch2 → pipeline → pipeline de novo`.
  Resultado: sem duplicação, perda de dado ou quebra em nenhuma execução. Contagens finais:
  Bronze 19.194 eventos / 450 clientes; Silver 18.658 eventos únicos, 450 registros de
  histórico de clientes (413 correntes, 37 com mudança de plano via SCD2).
- **Três bugs reais foram descobertos e corrigidos durante essa validação** (detalhes,
  causa raiz e evidências completas em `AJUSTES_PARTE2_TRANSFORM.md` — ler antes de mexer
  de novo nesses arquivos):
  1. `ingestion/api_client.py`: a mock API trata `since` como inclusivo (`>=`), fazendo o
     registro de fronteira voltar a cada execução — sem filtro client-side
     (`updated_at > since_param`), rodar a ingestão duas vezes no mesmo dia sobrescrevia
     o arquivo raw com um lote incompleto (perda de dado real, já reproduzida e corrigida).
  2. `transform/bronze_events.py`: o batch 2 introduz um campo novo no payload (`source_app`,
     schema drift proposital do desafio) — corrigido com evolução de schema controlada
     (`write.spark.accept-any-schema` + `mergeSchema`) e reordenação explícita das colunas
     do DataFrame (o Iceberg exige que a ordem bata com a da tabela).
  3. `transform/bronze_customers.py`: `is_active` chega como booleano nativo do Postgres;
     corrigido com cast explícito para `STRING` antes do write (a Bronze declara esse campo
     como STRING por design) + a mesma reordenação de colunas.
- `transform/config.py` não usa mais `python-dotenv` (removido — dentro do container Spark
  não existe `.env` para carregar; a chamada só existia para gerar uma dependência ausente
  na imagem).
- `README.md` atualizado com a documentação completa de `transform/` e diagramas Mermaid
  de execução (Bronze, Silver, arquitetura completa).

### Próximos passos (antes da entrega de quarta 16/09)

Conforme `docs/plano_desafio_tecnico.md`:

1. **Parte 3 — Gold + Trino**: tabelas gold e as 3 queries analíticas exigidas pelo
   enunciado, com resultados em CSV e interpretação escrita.
2. **Parte 4 — Qualidade**: 5+ checks de qualidade com severidade, persistidos em
   tabela Iceberg (não apenas logados).
3. **Parte 5 — Orquestração + Docs**: DAG do Airflow encadeando raw → bronze → silver →
   gold → checks; finalizar `README.md` e `ARCHITECTURE.md`.
4. Ambiente de infra atual já está no **batch 2** (não é mais batch 1) — qualquer teste
   novo de Gold/Qualidade deve levar isso em conta, ou resetar com `make clean && make up`
   se precisar repetir a sequência batch1→batch2 do zero.

Pontos de decisão já tomados (não reabrir sem motivo, ver `docs/plano_desafio_tecnico.md`):
watermark incremental em arquivo JSON no MinIO (não em tabela Iceberg nem Airflow Variable);
SCD2 completo na `silver.customers`; eventos com `customer_id` órfão são mantidos com flag
`_customer_exists`, nunca descartados silenciosamente.
