import boto3
import sys
from ingestion import config
from ingestion.watermark import read_watermark, write_watermark
from pathlib import Path

# Adiciona a raiz do repositório ao PATH do Python
sys.path.append(str(Path(__file__).resolve().parent.parent))

def run_test():
    # 1. Inicializa o cliente S3 com as credenciais do MinIO do config.py
    s3_client = boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
    )

    print("=== 1. Testando leitura de watermark inexistente ===")
    events_wm = read_watermark(s3_client, "events")
    print(f"Resultado (esperado None): {events_wm}\n")

    print("=== 2. Testando escrita do watermark de 'events' ===")
    write_watermark(s3_client, "events", "2026-03-11T14:00:00Z")
    events_wm = read_watermark(s3_client, "events")
    print(f"Resultado lido: {events_wm}\n")

    print("=== 3. Testando escrita de 'customers' (preservando 'events') ===")
    write_watermark(s3_client, "customers", "2026-03-10T09:00:00Z")
    
    events_wm = read_watermark(s3_client, "events")
    customers_wm = read_watermark(s3_client, "customers")
    
    print(f"Watermark 'events': {events_wm}")
    print(f"Watermark 'customers': {customers_wm}")

if __name__ == "__main__":
    run_test()