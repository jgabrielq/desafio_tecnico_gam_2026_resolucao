# Rodar o Pipeline — Comandos de Referência

Guia com todos os comandos para subir/validar o ambiente e rodar o pipeline
completo (raw → bronze → silver → qualidade → gold), em conjunto ou etapa
por etapa. Assume que a infra (MinIO, Postgres, mock API, Iceberg REST,
Spark, Trino) está definida em outro repositório, no path usado nesta
máquina:

```bash
export INFRA_DIR=/home/jgabrielq/repo_desafio_tecnico/desafio-pleno-2026-2
```

`run_full_pipeline_test.sh` exige `INFRA_DIR` no ambiente (não tem mais
default hardcoded) — use `export` como acima, ou passe inline
(`INFRA_DIR=... ./run_full_pipeline_test.sh`).

## Atalho — tudo em no máximo 3 comandos

Para rodar a sequência de teste completa exigida pelo desafio sem digitar
comando por comando, use o script `run_full_pipeline_test.sh` (raiz do
repo). Ele mesmo faz o deploy do código, roda ingestão → bronze → silver →
qualidade → gold para o batch 1 (duas vezes), dispara o `make batch2`, e
repete tudo para o batch 2 (duas vezes) — mostrando o resultado de cada
etapa e um resumo final.

```bash
cd "$INFRA_DIR" && make restart                          # 1. ambiente do zero
cd /home/jgabrielq/repo_resolucao_desafio_tecnico_gam \
    && pipenv install                                     # 2. dependências (uma vez só)
./run_full_pipeline_test.sh                               # 3. sequência de teste completa
```

O restante deste documento detalha os comandos individuais que o script
executa por baixo dos panos — útil para rodar uma camada isolada durante o
desenvolvimento, sem precisar da sequência inteira.

---

## 1. Ambiente

### 1.1 Subir o ambiente

```bash
cd "$INFRA_DIR"
make up
```

### 1.2 Validar que está tudo no ar (smoke test rápido)

```bash
cd "$INFRA_DIR"
make check
```

Deve retornar `12/12 verificações passaram`, incluindo o round-trip
Spark → Iceberg → MinIO → Trino.

### 1.3 Validação profunda (opcional)

```bash
cd "$INFRA_DIR"
make test-infra          # completo, ~2 min (inclui round-trip do Spark)
make test-infra-fast     # sem o round-trip do Spark, ~20s
```

### 1.4 Conferir containers manualmente

```bash
docker ps --format '{{.Names}}\t{{.Status}}\t{{.Ports}}'
```

Containers esperados: `dl-minio`, `dl-mock-api`, `dl-postgres`,
`dl-iceberg-rest`, `dl-spark`, `dl-trino`.

### 1.5 Em qual batch as fontes estão

```bash
cd "$INFRA_DIR"
make batch-state
```

### 1.6 Avançar para o batch 2 (simula o tempo passando)

```bash
cd "$INFRA_DIR"
make batch2
```

Libera dados novos na API e aplica mudanças de plano no Postgres.
**Atenção**: o Postgres só volta ao batch 1 com `make clean` (não existe
reset parcial para o CRM — `make reset-batch` só reseta a mock API).

### 1.7 Resetar o ambiente do zero (destrutivo)

```bash
cd "$INFRA_DIR"
make restart   # atalho para: make clean (apaga volumes) + make up
```

Use antes de repetir a sequência batch1→batch2 do zero, ou sempre que
precisar garantir que não há dado residual de testes anteriores.

---

## 2. Dependências Python (host)

O Python do sistema é "externally managed" — use Pipenv, não `pip install`
direto:

```bash
cd /home/jgabrielq/repo_resolucao_desafio_tecnico_gam
pipenv install
```

---

## 3. Etapa 1 — Ingestão (raw zone)

Roda no host (fora dos containers).

### 3.1 Rodar a ingestão

```bash
cd /home/jgabrielq/repo_resolucao_desafio_tecnico_gam
pipenv run python -m ingestion.ingest [YYYY-MM-DD]   # usa hoje se omitido
```

### 3.2 Rodar de novo no mesmo dia (teste de idempotência)

```bash
pipenv run python -m ingestion.ingest 2026-03-11
```

Sem dado novo desde o último watermark, deve logar "Nenhum evento novo" /
"Nenhum cliente novo" e **não** sobrescrever o arquivo raw do dia.

### 3.3 Testes manuais isolados dos módulos de ingestão

```bash
pipenv run python tests/test_storage.py
pipenv run python tests/test_watermark.py
pipenv run python tests/test_api_client.py
pipenv run python tests/test_postgres_client.py
```

### 3.4 Inspecionar o que foi gravado na raw zone (via MinIO)

```bash
docker exec dl-minio sh -c "mc alias set local http://localhost:9000 admin minioadmin && mc ls -r local/lakehouse/raw/"
```

### 3.5 Ler o conteúdo de um arquivo raw específico

```bash
docker exec dl-minio sh -c "mc cat local/lakehouse/raw/events/ingestion_date=2026-03-11/part-000.json.gz" | gunzip | head -5
```

### 3.6 Teste de conexão Spark → raw zone + catálogo Iceberg

```bash
docker cp tests/test_raw_connection.py dl-spark:/tmp/test_raw_connection.py
docker exec dl-spark spark-submit /tmp/test_raw_connection.py
```

---

## 4. Etapa 2 — Bronze e Silver (transform/)

Os jobs rodam **dentro do container Spark**. Copie a pasta `transform/`
inteira antes de cada mudança de código e use `PYTHONPATH=/tmp` para o
import `from transform import config` resolver.

### 4.1 Copiar o código para o container

```bash
docker cp transform dl-spark:/tmp/transform
```

### 4.2 Rodar a Bronze (events e customers)

```bash
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/bronze_events.py --ingestion_date 2026-03-11

docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/bronze_customers.py --ingestion_date 2026-03-11
```

### 4.3 Rodar a Silver (dedup de events + SCD2 de customers)

```bash
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/silver_events.py --ingestion_date 2026-03-11

docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/silver_customers.py --ingestion_date 2026-03-11
```

### 4.4 Teste de idempotência (rodar de novo para o mesmo dia)

```bash
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/bronze_events.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/bronze_customers.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/silver_events.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/silver_customers.py --ingestion_date 2026-03-11
```

As contagens (seção 7) devem ficar idênticas às da primeira execução.

---

## 5. Etapa 3 — Qualidade (quality/checks.py)

Roda sobre a Silver. `BLOCKING` interrompe o job com `RuntimeError`
(exit code não-zero); `WARNING` só registra.

### 5.1 Copiar o código para o container

```bash
docker cp quality dl-spark:/tmp/quality
```

### 5.2 Rodar os checks

```bash
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/quality/checks.py --ingestion_date 2026-03-11
```

**Atenção com a escolha do `--ingestion_date`**: o check de `freshness`
(BLOCKING) compara essa data contra o `MAX(occurred_at)` do próprio batch
na Silver. Use uma data coerente com a linha do tempo do batch sendo
processado (ex.: `2026-03-11` para o batch 1, `2026-09-01` para o batch 2 —
não a data literal do calendário real), senão o check pode falhar mesmo
com o pipeline saudável. Detalhes em `AJUSTES_PARTE4_QUALITY.md`.

### 5.3 Ver os últimos resultados

```bash
docker exec dl-trino trino --execute \
    "SELECT check_name, severity, status, metric_value, threshold_value, details
     FROM lakehouse.quality.check_results ORDER BY executed_at DESC LIMIT 5"
```

### 5.4 Teste de "não-idempotência" (intencional)

```bash
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/quality/checks.py --ingestion_date 2026-03-11
docker exec dl-trino trino --execute "SELECT COUNT(*) FROM lakehouse.quality.check_results"
```

A contagem total deve **crescer** a cada execução (5 novas linhas) — o log
de qualidade é histórico por design, não idempotente.

---

## 6. Etapa 4 — Gold (transform/gold.py)

Não recebe `--ingestion_date` — recalcula sempre do estado completo da
Silver.

### 6.1 Copiar o código atualizado e rodar

```bash
docker cp transform dl-spark:/tmp/transform

docker exec -e PYTHONPATH=/tmp dl-spark spark-submit \
    /tmp/transform/gold.py
```

### 6.2 Teste de idempotência

```bash
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/gold.py
```

As contagens das 3 tabelas gold devem ficar idênticas.

### 6.3 Gerar os CSVs de entrega (queries analíticas)

```bash
docker exec dl-trino trino --output-format TSV_HEADER \
    --execute "SELECT * FROM lakehouse.gold.top_clientes_30d" \
    > sql/resultados/query1_resultado.csv

docker exec dl-trino trino --output-format TSV_HEADER \
    --execute "SELECT plan, mes, avg_minutos_resposta, tickets_com_resposta,
               tickets_sem_resposta FROM lakehouse.gold.tempo_resposta_ticket
               ORDER BY mes, plan" \
    > sql/resultados/query2_resultado.csv

docker exec dl-trino trino --output-format TSV_HEADER \
    --execute "SELECT * FROM lakehouse.gold.retencao_coorte ORDER BY
               cohort_month, activity_month" \
    > sql/resultados/query3_resultado.csv
```

---

## 7. Validação via Trino (qualquer etapa)

### 7.1 Ver tabelas por camada

```bash
docker exec dl-trino trino --execute "SHOW TABLES IN lakehouse.bronze"
docker exec dl-trino trino --execute "SHOW TABLES IN lakehouse.silver"
docker exec dl-trino trino --execute "SHOW TABLES IN lakehouse.gold"
docker exec dl-trino trino --execute "SHOW TABLES IN lakehouse.quality"
```

### 7.2 Contagens por batch (Bronze)

```bash
docker exec dl-trino trino --execute \
    "SELECT COUNT(*), _batch_id FROM lakehouse.bronze.events GROUP BY _batch_id"
docker exec dl-trino trino --execute \
    "SELECT COUNT(*), _batch_id FROM lakehouse.bronze.customers GROUP BY _batch_id"
```

### 7.3 Contagens da Silver

```bash
docker exec dl-trino trino --execute "SELECT COUNT(*) FROM lakehouse.silver.events"
docker exec dl-trino trino --execute \
    "SELECT COUNT(*) FROM lakehouse.silver.customers WHERE is_current = true"
docker exec dl-trino trino --execute "SELECT COUNT(*) FROM lakehouse.silver.customers"
```

### 7.4 Contagens da Gold

```bash
docker exec dl-trino trino --execute "SELECT COUNT(*) FROM lakehouse.gold.top_clientes_30d"
# deve retornar exatamente 10

docker exec dl-trino trino --execute "SELECT COUNT(*) FROM lakehouse.gold.tempo_resposta_ticket"
docker exec dl-trino trino --execute "SELECT COUNT(*) FROM lakehouse.gold.retencao_coorte"
```

### 7.5 Resultados de qualidade (todos, ou só os que falharam)

```bash
docker exec dl-trino trino --execute \
    "SELECT check_name, severity, status, metric_value, details
     FROM lakehouse.quality.check_results ORDER BY executed_at DESC"

docker exec dl-trino trino --execute \
    "SELECT * FROM lakehouse.quality.check_results WHERE status = 'FAILED'
     ORDER BY executed_at DESC"
```

### 7.6 Schema de uma tabela

```bash
docker exec dl-trino trino --execute "DESCRIBE lakehouse.bronze.events"
docker exec dl-trino trino --execute "DESCRIBE lakehouse.silver.customers"
```

### 7.7 Rodar as 3 queries de entrega diretamente

```bash
docker exec dl-trino trino --file /dev/stdin < sql/query1_top10_clientes.sql
docker exec dl-trino trino --file /dev/stdin < sql/query2_tempo_resposta_ticket.sql
docker exec dl-trino trino --file /dev/stdin < sql/query3_retencao_coorte.sql
```

---

## 8. Pipeline completo — sequência de aceite do desafio

Sequência exigida pelo enunciado (nenhum passo pode duplicar, perder dado
ou quebrar):

```
pipeline (batch 1) → pipeline (batch 1 de novo) → make batch2 → pipeline → pipeline de novo
```

Onde "pipeline" = ingestão + bronze + silver + qualidade + gold. O jeito
mais simples de rodar essa sequência é o script (ver "Atalho" no topo
deste documento):

```bash
./run_full_pipeline_test.sh
```

Se preferir rodar manualmente (ex.: para depurar uma etapa específica),
os comandos são estes, na ordem:

```bash
cd /home/jgabrielq/repo_resolucao_desafio_tecnico_gam

# --- 0. Ambiente limpo (recomendado antes do teste definitivo) ---
cd "$INFRA_DIR" && make restart
cd /home/jgabrielq/repo_resolucao_desafio_tecnico_gam

# --- deploy do código para o container Spark ---
docker cp transform dl-spark:/tmp/transform
docker cp quality dl-spark:/tmp/quality

# --- 1. Pipeline, batch 1, execução 1 ---
pipenv run python -m ingestion.ingest 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/bronze_events.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/bronze_customers.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/silver_events.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/silver_customers.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/quality/checks.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/gold.py

# --- 2. Pipeline, batch 1, execução 2 (idempotência) — repete o bloco acima ---
pipenv run python -m ingestion.ingest 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/bronze_events.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/bronze_customers.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/silver_events.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/silver_customers.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/quality/checks.py --ingestion_date 2026-03-11
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/gold.py

# --- 3. Avançar para o batch 2 ---
cd "$INFRA_DIR" && make batch2
cd /home/jgabrielq/repo_resolucao_desafio_tecnico_gam

# --- 4. Pipeline, batch 2, execução 1 ---
# ingestion_date escolhido logo após o último evento sintético do batch 2
# (2026-08-31) — não a data literal do calendário real. Ver seção 5.2 sobre
# por que isso importa para o check de freshness.
pipenv run python -m ingestion.ingest 2026-09-01
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/bronze_events.py --ingestion_date 2026-09-01
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/bronze_customers.py --ingestion_date 2026-09-01
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/silver_events.py --ingestion_date 2026-09-01
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/silver_customers.py --ingestion_date 2026-09-01
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/quality/checks.py --ingestion_date 2026-09-01
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/gold.py

# --- 5. Pipeline, batch 2, execução 2 (idempotência) — repete o bloco acima ---
pipenv run python -m ingestion.ingest 2026-09-01
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/bronze_events.py --ingestion_date 2026-09-01
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/bronze_customers.py --ingestion_date 2026-09-01
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/silver_events.py --ingestion_date 2026-09-01
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/silver_customers.py --ingestion_date 2026-09-01
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/quality/checks.py --ingestion_date 2026-09-01
docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/gold.py

# --- 6. Validar tudo (seção 7 deste documento) ---
```

> Nota: use sempre a mesma `ingestion_date` (data de calendário) nos dois
> comandos de "execução 1" e "execução 2" de cada fase — é isso que testa a
> idempotência de verdade (mesmo dia, rodado duas vezes).

---

## 9. Referência rápida — tudo em uma tabela

| O que testar | Comando |
|---|---|
| **Tudo (3 comandos)** | `make restart` (infra) + `pipenv install` + `./run_full_pipeline_test.sh` |
| Ambiente no ar | `cd $INFRA_DIR && make check` |
| Batch atual | `cd $INFRA_DIR && make batch-state` |
| Avançar batch | `cd $INFRA_DIR && make batch2` |
| Reset total | `cd $INFRA_DIR && make restart` |
| Ingestão | `pipenv run python -m ingestion.ingest [data]` |
| Deploy do transform/ + quality/ | `docker cp transform dl-spark:/tmp/transform && docker cp quality dl-spark:/tmp/quality` |
| Bronze events | `docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/bronze_events.py --ingestion_date [data]` |
| Bronze customers | `docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/bronze_customers.py --ingestion_date [data]` |
| Silver events | `docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/silver_events.py --ingestion_date [data]` |
| Silver customers | `docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/silver_customers.py --ingestion_date [data]` |
| Qualidade (5 checks) | `docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/quality/checks.py --ingestion_date [data]` |
| Gold (todas as 3 tabelas) | `docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/transform/gold.py` |
| Ver tabelas de uma camada | `docker exec dl-trino trino --execute "SHOW TABLES IN lakehouse.<camada>"` |
| Contar linhas de uma tabela | `docker exec dl-trino trino --execute "SELECT COUNT(*) FROM lakehouse.<camada>.<tabela>"` |
