from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


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
    run_pipeline(context['ds'])


default_args = {
    'owner': 'engenharia_dados',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,   # substituir por on_failure_callback em prod
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
    start_date=datetime(2026, 3, 11),   # data do primeiro batch de dados
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
