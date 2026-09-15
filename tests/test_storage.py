import sys
import gzip
from pathlib import Path
from datetime import date

# Adiciona a raiz do projeto ao sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import logging
import boto3
from ingestion import config
from ingestion.storage import save_raw_batch, _to_ndjson

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def run_test():
    # 1. Dados fictícios de teste
    sample_records = [
        {"customer_id": "c_9991", "company_name": "Tech Corp", "is_active": True},
        {"customer_id": "c_9992", "company_name": "Data LLC", "is_active": False},
    ]
    ingestion_date = date.today().isoformat()

    print("=== 1. Testando serialização isolada (_to_ndjson) ===")
    ndjson_result = _to_ndjson(sample_records)
    print("NDJSON Gerado:\n", ndjson_result)

    print("\n=== 2. Testando gravação de lote no MinIO (save_raw_batch) ===")
    s3_client = boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
    )

    object_key = save_raw_batch(s3_client, sample_records, source="customers", ingestion_date=ingestion_date)
    print(f"Caminho do objeto gerado: {object_key}")

    print("\n=== 3. Validando leitura do arquivo recém-criado no MinIO ===")
    response = s3_client.get_object(Bucket=config.MINIO_BUCKET, Key=object_key)
    downloaded_content = gzip.decompress(response["Body"].read()).decode("utf-8")
    print("Conteúdo baixado do MinIO:\n", downloaded_content)


if __name__ == "__main__":
    run_test()