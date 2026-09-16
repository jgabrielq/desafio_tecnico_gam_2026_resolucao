#!/usr/bin/env bash
#
# run_full_pipeline_test.sh
#
# Executa a sequência de teste completa exigida pelo desafio, em todas as
# camadas (ingestão -> bronze -> silver -> qualidade -> gold):
#
#   pipeline(batch1) -> pipeline(batch1 de novo) -> make batch2 -> pipeline -> pipeline de novo
#
# Mostra o resultado de cada etapa conforme roda. Pré-requisito: ambiente já
# no ar e resetado (ver README.md / RODAR_PIPELINE.md).
#
# Uso:
#   ./run_full_pipeline_test.sh
#
# Variáveis de ambiente opcionais:
#   INFRA_DIR    caminho do repositório de infraestrutura (docker-compose)
#   BATCH1_DATE  ingestion_date usado para o batch 1 (default: 2026-03-11)
#   BATCH2_DATE  ingestion_date usado para o batch 2 (default: 2026-09-01,
#                escolhido logo após o último evento sintético do batch 2 —
#                ver AJUSTES_PARTE4_QUALITY.md sobre por que a escolha da
#                data importa para o check de freshness)

set -uo pipefail

INFRA_DIR="${INFRA_DIR:-/home/jgabrielq/repo_desafio_tecnico/desafio-pleno-2026-2}"
BATCH1_DATE="${BATCH1_DATE:-2026-03-11}"
BATCH2_DATE="${BATCH2_DATE:-2026-09-01}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

STEP_PASS=0
STEP_FAIL=0
FAILED_STEPS=()

header() {
    echo
    echo "════════════════════════════════════════════════════════════════"
    echo "  $1"
    echo "════════════════════════════════════════════════════════════════"
}

step_ok() {
    echo "[OK]      $1"
    STEP_PASS=$((STEP_PASS + 1))
}

step_fail() {
    echo "[FALHOU]  $1"
    STEP_FAIL=$((STEP_FAIL + 1))
    FAILED_STEPS+=("$1")
}

trino_query() {
    docker exec dl-trino trino --execute "$1" 2>/dev/null | tail -1 | tr -d '"'
}

deploy_code() {
    header "Deploy do código para o container Spark"
    docker exec dl-spark rm -rf /tmp/transform /tmp/quality
    docker cp "$REPO_DIR/transform" dl-spark:/tmp/transform
    docker cp "$REPO_DIR/quality" dl-spark:/tmp/quality
    step_ok "deploy transform/ + quality/ -> dl-spark:/tmp"
}

run_ingestion() {
    local date="$1" label="$2"
    header "Ingestão — $label (ingestion_date=$date)"
    local out
    out=$(cd "$REPO_DIR" && pipenv run python -m ingestion.ingest "$date" 2>&1)
    echo "$out" | grep -Ei "salvo com sucesso|Nenhum evento novo|Nenhum cliente novo|ERROR|Traceback"
    if echo "$out" | grep -qi "Pipeline finalizado com sucesso"; then
        step_ok "ingestão — $label"
    else
        step_fail "ingestão — $label"
    fi
}

run_spark_job() {
    local script="$1" label="$2" date="${3:-}"
    header "$label"
    local cmd=(docker exec -e PYTHONPATH=/tmp dl-spark spark-submit "/tmp/transform/$script")
    [ -n "$date" ] && cmd+=(--ingestion_date "$date")
    local out
    out=$("${cmd[@]}" 2>&1)
    local rc=$?
    echo "$out" | grep -Ei "_completed|ERROR|Traceback|RuntimeError|Quality checks:"
    if [ $rc -eq 0 ]; then
        step_ok "$label"
    else
        step_fail "$label"
    fi
}

run_quality_job() {
    local date="$1" label="$2"
    header "$label"
    local out
    out=$(docker exec -e PYTHONPATH=/tmp dl-spark spark-submit /tmp/quality/checks.py --ingestion_date "$date" 2>&1)
    local rc=$?
    echo "$out" | grep -Ei "Quality checks:|RuntimeError|Traceback"
    if [ $rc -eq 0 ]; then
        step_ok "$label (nenhum BLOCKING falhou)"
    else
        step_fail "$label (BLOCKING falhou — pipeline interrompido, como esperado)"
    fi
    echo "--- últimos 5 resultados persistidos ---"
    docker exec dl-trino trino --execute \
        "SELECT check_name, severity, status, metric_value, threshold_value FROM lakehouse.quality.check_results ORDER BY executed_at DESC LIMIT 5" \
        2>/dev/null
}

run_pipeline_layer_batch() {
    local date="$1" label="$2"
    run_ingestion "$date" "$label"
    run_spark_job "bronze_events.py" "Bronze events — $label" "$date"
    run_spark_job "bronze_customers.py" "Bronze customers — $label" "$date"
    run_spark_job "silver_events.py" "Silver events — $label" "$date"
    run_spark_job "silver_customers.py" "Silver customers — $label" "$date"
    run_quality_job "$date" "Qualidade — $label"
    run_spark_job "gold.py" "Gold — $label"

    header "Contagens após: $label"
    echo "bronze.events (total)      : $(trino_query 'SELECT COUNT(*) FROM lakehouse.bronze.events')"
    echo "bronze.customers (total)   : $(trino_query 'SELECT COUNT(*) FROM lakehouse.bronze.customers')"
    echo "silver.events              : $(trino_query 'SELECT COUNT(*) FROM lakehouse.silver.events')"
    echo "silver.customers (current) : $(trino_query 'SELECT COUNT(*) FROM lakehouse.silver.customers WHERE is_current = true')"
    echo "silver.customers (total)   : $(trino_query 'SELECT COUNT(*) FROM lakehouse.silver.customers')"
    echo "gold.top_clientes_30d      : $(trino_query 'SELECT COUNT(*) FROM lakehouse.gold.top_clientes_30d')"
    echo "gold.tempo_resposta_ticket : $(trino_query 'SELECT COUNT(*) FROM lakehouse.gold.tempo_resposta_ticket')"
    echo "gold.retencao_coorte       : $(trino_query 'SELECT COUNT(*) FROM lakehouse.gold.retencao_coorte')"
}

# ---------------------------------------------------------------------------

deploy_code

run_pipeline_layer_batch "$BATCH1_DATE" "batch 1, execução 1"
run_pipeline_layer_batch "$BATCH1_DATE" "batch 1, execução 2 (idempotência)"

header "make batch2"
( cd "$INFRA_DIR" && make batch2 )
step_ok "make batch2"

run_pipeline_layer_batch "$BATCH2_DATE" "batch 2, execução 1"
run_pipeline_layer_batch "$BATCH2_DATE" "batch 2, execução 2 (idempotência)"

header "RESUMO"
echo "Etapas OK:      $STEP_PASS"
echo "Etapas FALHOU:  $STEP_FAIL"
if [ "$STEP_FAIL" -gt 0 ]; then
    echo "Etapas que falharam:"
    for s in "${FAILED_STEPS[@]}"; do
        echo "  - $s"
    done
    exit 1
fi
echo "Sequência completa do desafio rodou sem duplicação, perda de dado ou quebra."
exit 0
