import gzip
import json
import logging
from ingestion import config

logger = logging.getLogger(__name__)


def _to_ndjson(records: list[dict]) -> str:
    """
    Concatena cada registro extraído como um JSON serializado dentro
    de um único arquivo no formato NDJSON (JSON Lines).
    """
    if not records:
        logger.info("Nenhum registro para concatenar.")
        return ""

    try:
        ndjson_content = "\n".join(
            json.dumps(record, ensure_ascii=False, default=str) for record in records
        )
        return ndjson_content

    except Exception as e:
        logger.error(f"Erro ao serializar registros: {e}")
        raise


def save_raw_batch(s3_client, records: list[dict], source: str, ingestion_date: str) -> str:
    """
    Persiste a lista de registros no MinIO. Caminho gerado no Lakehouse:
    raw/<source>/ingestion_date=<ingestion_date>/part-000.json.gz
    """
    ndjson_content = _to_ndjson(records)

    if not ndjson_content:
        logger.info(f"Nenhum registro para salvar na fonte '{source}'.")
        return ""

    # Padrão Hive Partitioning no S3/MinIO
    object_key = f"raw/{source}/ingestion_date={ingestion_date}/part-000.json.gz"

    try:
        body = gzip.compress(ndjson_content.encode("utf-8"))
        s3_client.put_object(
            Bucket=config.MINIO_BUCKET,
            Key=object_key,
            Body=body,
            ContentType="application/gzip",
        )

        logger.info(
            f"Lote de '{source}' salvo com sucesso em "
            f"s3://{config.MINIO_BUCKET}/{object_key} ({len(records)} registros)"
        )
        return object_key

    except Exception as e:
        logger.error(f"Erro ao salvar lote de '{source}' no MinIO: {e}")
        raise