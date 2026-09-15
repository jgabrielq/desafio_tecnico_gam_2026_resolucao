import logging
import sys
import boto3
from ingestion import config
from ingestion.api_client import fetch_all_events
from ingestion.postgres_client import fetch_customers
from ingestion.storage import save_raw_batch
from ingestion.watermark import read_watermark, write_watermark

# Configuração de logging estruturado
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingestion.orchestrator")


def run_pipeline(ingestion_date: str) -> None:
    logger.info(f"Iniciando pipeline de ingestão | ingestion_date: {ingestion_date}")

    # Inicializa cliente S3 / MinIO
    s3_client = boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
    )

    # -------------------------------------------------------------------------
    # FONTE 1: REST API (Events)
    # -------------------------------------------------------------------------
    logger.info("=== Processando Fonte: API Events ===")
    events_watermark = read_watermark(s3_client, source="events") # Carrega a nossa estrutura de controle
    
    # Extração
    events, api_metrics = fetch_all_events(since=events_watermark)
    
    # Persistência na Raw Zone
    if events:
        save_raw_batch(s3_client, records=events, source="events", ingestion_date=ingestion_date)
        
        # Atualiza o watermark com o maior 'updated_at' do lote
        latest_event_ts = max(e["updated_at"] for e in events if "updated_at" in e)
        write_watermark(s3_client, source="events", last_updated_at=latest_event_ts)
    else:
        logger.info("Nenhum evento novo retornado pela API.")

    # -------------------------------------------------------------------------
    # FONTE 2: PostgreSQL (CRM Customers)
    # -------------------------------------------------------------------------
    logger.info("=== Processando Fonte: CRM Customers (Postgres) ===")
    customers_watermark = read_watermark(s3_client, source="customers")
    
    # Extração
    customers = fetch_customers(since=customers_watermark)
    
    # Persistência na Raw Zone
    if customers:
        save_raw_batch(s3_client, records=customers, source="customers", ingestion_date=ingestion_date)
        
        # Atualiza o watermark com o maior 'updated_at' do lote
        latest_customer_ts = max(c["updated_at"] for c in customers if "updated_at" in c)
        write_watermark(s3_client, source="customers", last_updated_at=latest_customer_ts)
    else:
        logger.info("Nenhum cliente novo retornado pelo PostgreSQL.")

    logger.info("ingestion_completed", extra={
        "ingestion_date": ingestion_date,
        "events_records": len(events) if events else 0,
        "customers_records": len(customers) if customers else 0,
    })

    logger.info(f"Pipeline finalizado com sucesso | ingestion_date: {ingestion_date}")


if __name__ == "__main__":
    from datetime import date
    ingestion_date = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    run_pipeline(ingestion_date)