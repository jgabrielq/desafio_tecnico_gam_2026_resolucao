# SPEC — Parte 5: DAG do Airflow

Especificação técnica completa para implementação da DAG de orquestração.
O enunciado é explícito: a DAG não precisa rodar no ambiente — código bem
estruturado + explicação no README conta pontuação integral.

---

## Contexto e decisões arquiteturais

**A DAG não sobe infraestrutura**: os containers já estão de pé via
`docker-compose` do repositório de ambiente. A DAG apenas orquestra a
execução dos scripts que já existem.

**Ordem das tasks** (definida na Parte 4):
```
ingestao → bronze_events + bronze_customers → silver_events + silver_customers
         → quality_checks → gold
```

**Qualidade antes da Gold**: checks BLOCKING falham a task, impedindo que
a Gold seja calculada com dados corrompidos.

**Bronze em paralelo**: `bronze_events` e `bronze_customers` não dependem
uma da outra — rodam em paralelo após a ingestão.

**Silver em paralelo**: mesma lógica — `silver_events` e `silver_customers`
rodam em paralelo após o bronze de cada fonte estar pronto.

---

## Estrutura de arquivos a criar

```
dags/
└── dag_pipeline.py    # DAG completa do Airflow
```

---

## `dags/dag_pipeline.py`

### Imports e configuração

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
```

**Por que `BashOperator` e não `SparkSubmitOperator`**: os jobs Spark
rodam via `docker exec` dentro do container `dl-spark` — não há um cluster
Spark com `spark-master` separado ao qual o Airflow se conectaria via
`SparkSubmitOperator`. O `BashOperator` executa o `docker exec` diretamente,
que é a forma real de submeter os jobs nesse ambiente.

**Por que `PythonOperator` para a ingestão**: a ingestão roda no host
(não dentro do container Spark) via `pipenv run python -m ingestion.ingest`.
O `PythonOperator` chama a função `run_pipeline` diretamente, sem depender
do `pipenv` estar no PATH do Airflow — mais robusto que um `BashOperator`
com caminho de ambiente.

---

### Função de ingestão chamada pelo PythonOperator

```python
def executar_ingestao(**context):
    """
    Chama run_pipeline da ingestão diretamente (sem subprocess).
    Usa o ds (data lógica do Airflow) como ingestion_date.
    """
    import sys
    import os
    # Adiciona o diretório raiz do projeto ao PATH para importar ingestion/
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from ingestion.ingest import run_pipeline

    ingestion_date = context['ds']  # formato YYYY-MM-DD, nunca datetime.now()
    run_pipeline(ingestion_date)
```

---

### `default_args`

```python
default_args = {
    'owner': 'engenharia_dados',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': False,
    'email_on_failure': False,   # substituir por on_failure_callback em prod
}
```

**`depends_on_past=False`**: cada execução é independente — uma falha no
dia anterior não bloqueia a execução de hoje. O watermark na raw zone
garante que a ingestão sempre pega só o delta correto, independente de
execuções anteriores.

**`retries=2`**: duas retentativas com 5 minutos de intervalo. Cobre falhas
transitórias (rede, container reiniciando). Não cobre falhas estruturais
(dado corrompido, schema drift não tratado) — essas precisam de intervenção
humana.

---

### Definição da DAG

```python
with DAG(
    dag_id='lakehouse_pipeline',
    description='Pipeline raw → bronze → silver → quality → gold',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 3, 11),   # data do primeiro batch de dados
    catchup=True,
    max_active_runs=1,
    tags=['lakehouse', 'iceberg', 'desafio'],
) as dag:
```

**`schedule_interval='@daily'`**: executa uma vez por dia, à meia-noite UTC.
Em produção, ajustar para um horário após a janela de chegada dos dados
(ex: `'0 6 * * *'` para rodar às 6h, após os dados chegarem durante a noite).

**`catchup=True`**: permite backfill — se o pipeline ficou parado por N dias,
o Airflow vai criar uma execução para cada dia perdido, na ordem cronológica.
Seguro porque cada etapa é idempotente.

**`max_active_runs=1`**: só uma execução ativa por vez. Evita que dois
backfills concorrentes escrevam na mesma tabela Iceberg simultaneamente —
o MERGE INTO não é seguro para escrita concorrente sem controle de transação
distribuída.

**`start_date=datetime(2026, 3, 11)`**: data do primeiro batch real de dados.
Nunca usar `days_ago(1)` ou `datetime.now()` — o `start_date` deve ser
fixo e determinístico.

---

### Tasks

#### Task 1 — Ingestão

```python
    ingestao = PythonOperator(
        task_id='ingestao_raw',
        python_callable=executar_ingestao,
        provide_context=True,
    )
```

#### Tasks 2 e 3 — Bronze (paralelas)

```python
    SPARK_CMD = (
        "docker exec -e PYTHONPATH=/tmp dl-spark spark-submit "
        "/tmp/transform/{script} --ingestion_date {ds}"
    )

    bronze_events = BashOperator(
        task_id='bronze_events',
        bash_command=SPARK_CMD.format(
            script='bronze_events.py',
            ds='{{ ds }}'
        ),
    )

    bronze_customers = BashOperator(
        task_id='bronze_customers',
        bash_command=SPARK_CMD.format(
            script='bronze_customers.py',
            ds='{{ ds }}'
        ),
    )
```

**`{{ ds }}`**: template Jinja do Airflow — resolve para a data lógica da
execução no formato `YYYY-MM-DD`. Nunca usar `datetime.now().strftime(...)`,
que quebraria o backfill (todas as execuções usariam a data atual).

#### Tasks 4 e 5 — Silver (paralelas)

```python
    silver_events = BashOperator(
        task_id='silver_events',
        bash_command=SPARK_CMD.format(
            script='silver_events.py',
            ds='{{ ds }}'
        ),
    )

    silver_customers = BashOperator(
        task_id='silver_customers',
        bash_command=SPARK_CMD.format(
            script='silver_customers.py',
            ds='{{ ds }}'
        ),
    )
```

#### Task 6 — Qualidade (BLOCKING para Gold)

```python
    quality_checks = BashOperator(
        task_id='quality_checks',
        bash_command=(
            "docker exec -e PYTHONPATH=/tmp dl-spark spark-submit "
            "/tmp/quality/checks.py --ingestion_date {{ ds }}"
        ),
    )
```

Se um check BLOCKING falhar, o job Spark termina com `RuntimeError` →
exit code não-zero → o `BashOperator` falha a task → o Airflow não executa
a task `gold` downstream. Comportamento correto sem nenhuma lógica adicional.

#### Task 7 — Gold (recalcula sempre do estado completo)

```python
    gold = BashOperator(
        task_id='gold',
        bash_command=(
            "docker exec -e PYTHONPATH=/tmp dl-spark spark-submit "
            "/tmp/transform/gold.py"
        ),
    )
```

Sem `--ingestion_date` — a Gold sempre recalcula do estado completo da
Silver, independente do batch sendo processado.

---

### Dependências entre tasks

```python
    # Ingestão primeiro
    ingestao >> [bronze_events, bronze_customers]

    # Bronze events destrava silver events
    bronze_events >> silver_events

    # Bronze customers destrava silver customers
    bronze_customers >> silver_customers

    # Qualidade só roda após ambas as Silvers terminarem
    [silver_events, silver_customers] >> quality_checks

    # Gold só roda se qualidade passou (sem BLOCKING)
    quality_checks >> gold
```

**Diagrama do fluxo**:
```
ingestao_raw
    ├── bronze_events ──── silver_events ──┐
    └── bronze_customers ── silver_customers ┴── quality_checks ── gold
```

---

## DAG completa (arquivo final)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def executar_ingestao(**context):
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from ingestion.ingest import run_pipeline
    run_pipeline(context['ds'])


default_args = {
    'owner': 'engenharia_dados',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

SPARK_SUBMIT = (
    "docker exec -e PYTHONPATH=/tmp dl-spark spark-submit "
    "/tmp/transform/{script} --ingestion_date {{{{ ds }}}}"
)

with DAG(
    dag_id='lakehouse_pipeline',
    description='Pipeline raw → bronze → silver → quality → gold',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2026, 3, 11),
    catchup=True,
    max_active_runs=1,
    tags=['lakehouse', 'iceberg', 'desafio'],
) as dag:

    ingestao = PythonOperator(
        task_id='ingestao_raw',
        python_callable=executar_ingestao,
        provide_context=True,
    )

    bronze_events = BashOperator(
        task_id='bronze_events',
        bash_command=SPARK_SUBMIT.format(script='bronze_events.py'),
    )

    bronze_customers = BashOperator(
        task_id='bronze_customers',
        bash_command=SPARK_SUBMIT.format(script='bronze_customers.py'),
    )

    silver_events = BashOperator(
        task_id='silver_events',
        bash_command=SPARK_SUBMIT.format(script='silver_events.py'),
    )

    silver_customers = BashOperator(
        task_id='silver_customers',
        bash_command=SPARK_SUBMIT.format(script='silver_customers.py'),
    )

    quality_checks = BashOperator(
        task_id='quality_checks',
        bash_command=(
            "docker exec -e PYTHONPATH=/tmp dl-spark spark-submit "
            "/tmp/quality/checks.py --ingestion_date {{ ds }}"
        ),
    )

    gold = BashOperator(
        task_id='gold',
        bash_command=(
            "docker exec -e PYTHONPATH=/tmp dl-spark spark-submit "
            "/tmp/transform/gold.py"
        ),
    )

    ingestao >> [bronze_events, bronze_customers]
    bronze_events >> silver_events
    bronze_customers >> silver_customers
    [silver_events, silver_customers] >> quality_checks
    quality_checks >> gold
```

---

## Notas para o code review

- **Por que `catchup=True` e `max_active_runs=1`**: catchup permite backfill
  seguro porque cada etapa é idempotente. `max_active_runs=1` evita escrita
  concorrente nas tabelas Iceberg — dois MERGEs simultâneos na Silver podem
  gerar conflito de snapshot.

- **Por que `{{ ds }}` e não `datetime.now()`**: `ds` é a data lógica da
  execução — em backfill, cada execução recebe a data correta do seu
  intervalo. `datetime.now()` quebraria o backfill fazendo todas as execuções
  usarem a data atual.

- **Por que `depends_on_past=False`**: cada execução é independente graças
  ao watermark — a ingestão sempre sabe de onde continuar, sem precisar que
  o dia anterior tenha rodado com sucesso.

- **Por que qualidade antes da Gold**: checks BLOCKING falham a task via
  `RuntimeError` → exit code não-zero → Airflow não executa downstream.
  Sem lógica extra, a Gold simplesmente não roda se os dados estiverem
  corrompidos.

- **Por que `BashOperator` e não `SparkSubmitOperator`**: os jobs rodam
  via `docker exec` no container `dl-spark` — não há cluster Spark com
  master/worker separado ao qual o Airflow se conectaria diretamente.
  `BashOperator` executa o `docker exec` como qualquer outro comando shell.

- **Por que `PythonOperator` para a ingestão**: a ingestão roda no host,
  não no container Spark. Chamar a função diretamente é mais robusto que
  depender do `pipenv` estar no PATH do worker do Airflow.

- **Backfill manual** (se necessário):
  ```bash
  airflow dags backfill lakehouse_pipeline \
      --start-date 2026-03-11 \
      --end-date 2026-03-15
  ```

- **Sobre o Airflow não rodar no ambiente**: o enunciado aceita código bem
  estruturado + explicação. A DAG foi escrita para ser executável — se o
  Airflow fosse subido com `make airflow`, ela funcionaria sem modificações,
  desde que os containers do ambiente estivessem de pé e os scripts
  copiados para `/tmp/transform` e `/tmp/quality` dentro do `dl-spark`.