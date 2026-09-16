# CLAUDE.md

Guia de contexto para retomar este repositório em sessões futuras do Claude Code.

## Sobre o projeto

Desafio técnico de engenharia de dados: pipeline de lakehouse on-premises
(MinIO · Iceberg · Spark · Trino · Airflow), com ingestão raw → bronze → silver → gold,
checks de qualidade e um DAG de orquestração. Prazo de entrega: **quarta-feira 16/09 às 16h**.

Documentos de referência (já existentes, não recriar):
- `docs/plano_desafio_tecnico.md` — cronograma e decisões de arquitetura para todas as partes.
- `docs/ARQUITETURA.md` — respostas às 6 perguntas obrigatórias do enunciado (particionamento, idempotência, SCD2, volume 100x, etc.).
- `docs/DISCUSSOES_PARTE1.md`, `docs/DISCUSSOES_PART2_a.md`, `docs/DISCUSSOES_PART3.MD`, `docs/DISCUSSOES_PARTE4.md` — notas de discussão por parte.
- `docs/PRIMER_ICEBERG.md`, `docs/CONCEITOS_REVISAR.md`, `docs/SCHEMA_DRIFT.md`, `docs/ENVIRONMENT_FIXES.md`, `docs/README_ambiente.md`.
- `SPEC_PART1_INGESTION.md`, `SPEC_PART2_a_TRANSFORM.md`, `SPEC_PART2_b_TRANSFOMR.md`, `SPEC_PART3_GOLD.MD`, `SPEC_PART4_QUALITY_CHECK.md`, `SPEC_PART5_DAG.md` — especificações técnicas de cada parte (ingestão, Bronze, Silver, Gold, Qualidade, DAG). As specs da Bronze e Qualidade têm notas inline documentando os ajustes que precisaram ser feitos sobre o texto original.
- `AJUSTES_PARTE1_INGESTION.md`, `AJUSTES_PARTE2_TRANSFORM.md`, `AJUSTES_PARTE4_QUALITY.md` — problemas reais descobertos ao validar cada parte (causa raiz + correção). Ler antes de mexer de novo nos arquivos que eles cobrem.
- `RODAR_PIPELINE.md` — todos os comandos CLI para rodar/validar o ambiente e cada etapa do pipeline, isolada ou na sequência completa de aceite do desafio. Referência canônica de comandos — preferir isso a redigitar comandos do zero.
- `run_full_pipeline_test.sh` (raiz do repo) — script que roda a sequência de teste completa (batch1×2 → batch2 → batch2×2, todas as camadas) e mostra o resultado de cada etapa. É o comando 3 do fluxo "3 comandos" pedido pelo desafio.
- `README.md` — documentação de `ingestion/`, `transform/` e `quality/` (inclui diagramas Mermaid de execução de cada camada e da arquitetura completa).

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

Teste de leitura da raw zone + catálogo Iceberg via Spark (`tests/test_raw_connection.py`,
movido de `ingestion/` para `tests/`) — **roda dentro do container**, não localmente:

```bash
docker cp tests/test_raw_connection.py dl-spark:/tmp/test_raw_connection.py
docker exec dl-spark spark-submit /tmp/test_raw_connection.py
```

Jobs de transformação (`transform/` — Bronze, Silver, Gold) e de qualidade
(`quality/`) rodam dentro do container Spark. Copiar a pasta inteira (não só
um arquivo) e usar `PYTHONPATH=/tmp` para os imports `from transform import
config` / `from quality import checks` resolverem:

```bash
docker cp transform dl-spark:/tmp/transform
docker cp quality dl-spark:/tmp/quality

docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/bronze_events.py --ingestion_date YYYY-MM-DD
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/bronze_customers.py --ingestion_date YYYY-MM-DD
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/silver_events.py --ingestion_date YYYY-MM-DD
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/silver_customers.py --ingestion_date YYYY-MM-DD

# Gold — sem --ingestion_date, sempre recalcula do estado completo da Silver
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/gold.py

# Qualidade — roda sobre a Silver, persiste em lakehouse.quality.check_results
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/quality/checks.py --ingestion_date YYYY-MM-DD
```

Validar resultados via Trino:

```bash
docker exec dl-trino trino --execute "SELECT COUNT(*), _batch_id FROM lakehouse.bronze.events GROUP BY _batch_id"
docker exec dl-trino trino --execute "SELECT COUNT(*) FROM lakehouse.silver.customers WHERE is_current = true"
docker exec dl-trino trino --execute "SELECT COUNT(*) FROM lakehouse.gold.top_clientes_30d"   # deve ser 10
docker exec dl-trino trino --execute "SELECT check_name, status FROM lakehouse.quality.check_results ORDER BY executed_at DESC LIMIT 5"
```

Comandos completos (incluindo a sequência de aceite do desafio) em `RODAR_PIPELINE.md`.

**Atalho em 3 comandos** (o desafio pede que a entrega seja validável assim):
`make restart` (infra, reset do zero) + `pipenv install` (uma vez) +
`./run_full_pipeline_test.sh` (raiz deste repo — roda a sequência completa
batch1×2 → batch2 → batch2×2 em todas as camadas, mostrando o resultado de
cada etapa e um resumo final; termina com exit code não-zero se algo falhar).

Para avançar as fontes para o batch 2 (simula o tempo passando) e resetar o ambiente
do zero quando precisar de um estado limpo (destrutivo — apaga volumes Docker):

```bash
cd /home/jgabrielq/repo_desafio_tecnico/desafio-pleno-2026-2   # repo de infra
make batch2      # libera batch 2 na API + aplica mudanças no Postgres (irreversível sem make clean)
make restart     # atalho para make clean (apaga volumes) + make up (sobe tudo de novo e valida)
```

## Estado atual do trabalho (última sessão)

**Todas as 5 partes implementadas: Ingestão, Bronze, Silver, Gold, Qualidade e DAG.**
**Falta só uma revisão final antes da entrega (prazo: quarta 16/09 às 16h — hoje).**

Repositório git inicializado nesta sessão (`git init` + commits incrementais); antes
disso o diretório não era um repo.

- `ingestion/` completo: `config.py`, `api_client.py` (paginação + retry 429/5xx +
  filtro client-side do boundary do watermark), `postgres_client.py`, `watermark.py`,
  `storage.py` (NDJSON + gzip, particionamento Hive por `ingestion_date`), `ingest.py`
  (orquestrador).
- `transform/` completo: `config.py`, `bronze_events.py`, `bronze_customers.py`,
  `silver_events.py`, `silver_customers.py` (SCD Tipo 2), `gold.py` (3 tabelas
  analíticas: `top_clientes_30d`, `tempo_resposta_ticket`, `retencao_coorte`).
- `quality/checks.py` completo: 5 checks (unicidade, integridade referencial,
  volumetria, domínios, freshness) persistidos em `lakehouse.quality.check_results`,
  com `BLOCKING` interrompendo via `RuntimeError` e `WARNING` só registrando.
- `sql/query{1,2,3}_*.sql` + `sql/resultados/*.csv`: as 3 queries de entrega da Gold,
  com interpretação escrita (baseada em números reais) como comentário no final de
  cada `.sql`.
- Teste de conexão Spark → raw zone + catálogo Iceberg (`tests/test_raw_connection.py`)
  validado dentro do container `dl-spark`.
- **Sequência de teste completa do desafio executada e validada, agora nas 5 camadas**
  (ingestão → bronze → silver → qualidade → gold), com o ambiente resetado do zero
  (`make restart`) antes do teste definitivo: `pipeline (batch1) → pipeline (batch1 de
  novo) → make batch2 → pipeline → pipeline de novo`. Resultado: 30/30 etapas OK, sem
  duplicação, perda de dado ou quebra em nenhuma execução. Contagens finais: Bronze
  19.194 eventos / 450 clientes; Silver 18.658 eventos únicos, 450 registros de histórico
  de clientes (413 correntes, 37 com mudança de plano via SCD2); Qualidade 3 PASSED / 2
  FAILED no batch 2 (nenhum BLOCKING, WARNINGs esperados); Gold 10/12/90 linhas nas 3
  tabelas — tudo idêntico entre as duas execuções de cada batch.
- **`run_full_pipeline_test.sh` criado** (raiz do repo): automatiza essa sequência
  inteira (deploy do código + as 5 camadas × 4 execuções), mostrando o resultado de cada
  etapa e um resumo final (`Etapas OK` / `Etapas FALHOU`, exit code não-zero se algo
  falhar). Usa `ingestion_date=2026-03-11` para o batch 1 e `2026-09-01` para o batch 2
  (não a data literal do calendário real — importante para o check de `freshness`).
  Junto com `make restart` (infra) e `pipenv install`, forma o fluxo de "3 comandos"
  que o desafio pede para validar a entrega — documentado em destaque no topo do
  `README.md` e do `RODAR_PIPELINE.md`.
- **Bugs reais descobertos e corrigidos durante as validações** (causa raiz e evidências
  completas em `AJUSTES_PARTE2_TRANSFORM.md` e `AJUSTES_PARTE4_QUALITY.md` — ler antes de
  mexer de novo nesses arquivos):
  1. `ingestion/api_client.py`: a mock API trata `since` como inclusivo (`>=`), fazendo o
     registro de fronteira voltar a cada execução — corrigido com filtro client-side
     (`updated_at > since_param`).
  2. `transform/bronze_events.py`: o batch 2 introduz um campo novo no payload
     (`source_app`, schema drift proposital) — corrigido com evolução de schema
     controlada (`write.spark.accept-any-schema` + `mergeSchema`) e reordenação
     explícita das colunas do DataFrame (o Iceberg exige que a ordem bata com a da tabela).
  3. `transform/bronze_customers.py`: `is_active` chega como booleano nativo do Postgres;
     corrigido com cast explícito para `STRING` antes do write + a mesma reordenação de colunas.
  4. `quality/checks.py` — `check_dominios`: a lista de `event_type` válidos da spec não
     incluía `feature_used` (tipo legítimo do dataset) — corrigido adicionando à lista.
  5. `quality/checks.py` — `check_freshness`: comparava contra o relógio real
     (`_ingested_at`), o que faria esse check `BLOCKING` falhar permanentemente com um
     dataset sintético fixo no tempo — corrigido para comparar contra o `ingestion_date`
     do próprio batch (mesmo princípio da Gold: `MAX(occurred_at)`, não `CURRENT_DATE`).
- `transform/config.py` não usa mais `python-dotenv` (dentro do container Spark não existe
  `.env` para carregar; a chamada só existia para gerar uma dependência ausente na imagem).
- `SPEC_PART2_a_TRANSFORM.md` e `SPEC_PART2_b_TRANSFOMR.md` atualizados com notas inline
  documentando os ajustes 2-3 acima (o mecanismo real de evolução de schema não estava na
  spec original).
- `README.md` atualizado com a documentação completa de `transform/` e `quality/`, e
  diagramas Mermaid de execução de cada camada (Ingestão, Bronze, Silver, Gold, Qualidade)
  + arquitetura completa.
- `RODAR_PIPELINE.md` criado: referência única com todos os comandos CLI de
  ambiente/execução/validação, camada por camada ou na sequência completa de aceite.
- `dags/dag_pipeline.py`: DAG do Airflow (`lakehouse_pipeline`) implementada conforme
  `SPEC_PART5_DAG.md`, sem nenhuma adaptação sobre a spec (código consolidado final
  transcrito literalmente). Encadeia `ingestao_raw` (`PythonOperator`, chama
  `run_pipeline` direto) → `bronze_events`/`bronze_customers` em paralelo
  (`BashOperator` + `docker exec`) → `silver_events`/`silver_customers` em paralelo →
  `quality_checks` → `gold`. **Não foi executada** (o enunciado aceita código bem
  estruturado + explicação sem precisar rodar no ambiente — não há Airflow no ar; o
  `docker-compose` da infra tem um serviço `airflow` opcional via `make airflow`, mas
  ele monta `./dags` do **repo de infra**, não deste repo — para testar de verdade
  seria preciso copiar `dag_pipeline.py` para lá). Validado apenas com
  `py_compile`/`ast.parse` (sintaxe OK) e verificação manual de que o truque de
  `.format()` com `{{{{ ds }}}}` resolve corretamente para o template Jinja `{{ ds }}`.
  Explicação completa das decisões de design em `README.md` (seção "Orquestração").
- Reorganização manual do usuário: `INGESTIONS_FIXES.md` → `AJUSTES_PARTE1_INGESTION.md`;
  `ingestion/test_raw_connection.py` → `tests/test_raw_connection.py`; `PART2_TEST.md`
  removido (instrução pontual já concluída); `docs/ARQUITETURA.md` preenchido com as
  respostas às 6 perguntas obrigatórias do enunciado.

### Próximos passos

Todas as 5 partes do desafio estão implementadas. Antes de considerar a entrega
fechada, vale:

1. Revisão final de tudo (README, ARQUITETURA.md, specs com notas de ajuste, código) —
   nenhuma parte pendente identificada, mas não houve uma passada de revisão completa
   de ponta a ponta nesta sessão.
2. Ambiente de infra atual está no **batch 2**, com dado de teste já processado em todas
   as camadas (incluindo `lakehouse.quality.check_results`, que não é idempotente — cada
   execução de teste soma linhas novas). Rodar `make restart` antes de uma validação final
   "limpa" para a entrega — o `run_full_pipeline_test.sh` não reseta a infra sozinho, só
   assume que ela já está limpa quando começa (ele mesmo dispara o `make batch2` no meio
   da sequência).
3. Ao rodar `quality/checks.py` de novo, escolher `--ingestion_date` de forma coerente com
   a linha do tempo do batch sendo processado (não a data literal do calendário real) —
   ver a nuance documentada em `AJUSTES_PARTE4_QUALITY.md` sobre o check de freshness.
4. Se quiser demonstrar a DAG rodando de verdade (não exigido pelo enunciado): copiar
   `dags/dag_pipeline.py` para a pasta `dags/` do repo de infra e subir com `make airflow`.

Pontos de decisão já tomados (não reabrir sem motivo, ver `docs/plano_desafio_tecnico.md`):
watermark incremental em arquivo JSON no MinIO (não em tabela Iceberg nem Airflow Variable);
SCD2 completo na `silver.customers`; eventos com `customer_id` órfão são mantidos com flag
`_customer_exists`, nunca descartados silenciosamente; qualidade persistida em tabela Iceberg
(não apenas logada), com log intencionalmente não-idempotente para auditoria histórica.
