import json
import logging

from datetime import datetime, timezone
from botocore.exceptions import ClientError
from ingestion import config

logger = logging.getLogger(__name__)

def read_watermark(s3_client, source: str) -> str | None:
    """Retorna o último 'last_updated_at' de cada fonte ('customers' ou 'events')"""
    try:
        response = s3_client.get_object(
            Bucket=config.MINIO_BUCKET, 
            Key=config.WATERMARK_KEY
        )
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)
        last_updated_at = data.get(source, {}).get("last_updated_at")

        logger.info(
            f"Watermark lido para '{source}': {last_updated_at or 'Nenhum (primeira execução)'}"
        )
        return last_updated_at

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("NoSuchKey", "404"):
            logger.info(
                f"Arquivo de watermark '{config.WATERMARK_KEY}' não encontrado. Iniciando primeira execução."
            )
            return None
        logger.error(f"Erro ao ler watermark no MinIO para '{source}': {e}")
        raise
    except json.JSONDecodeError:
        logger.warning(
            f"Arquivo '{config.WATERMARK_KEY}' contém JSON inválido. Tratando como primeira execução."
        )
        return None
    
def write_watermark(s3_client, source: str, last_updated_at: str) -> None:
    """
    Atualiza o watermark de uma fonte específica preservando as demais. Grava no formato ISO 8601 UTC com o sufixo Z.
    """
    data = {}

    # Tenta carregar o estado atual para não sobrescrever outras fontes, mantendo o estado da fonte que não será atualizada
    try:
        response = s3_client.get_object(
            Bucket=config.MINIO_BUCKET, Key=config.WATERMARK_KEY
        )
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)
        
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code not in ("NoSuchKey", "404"):
            logger.error(f"Erro ao obter watermark existente antes da atualização: {e}")
            raise
        
    except json.JSONDecodeError:
        logger.warning(
            f"Arquivo '{config.WATERMARK_KEY}' continha JSON inválido ao tentar atualizar. Reiniciando dicionário de estado."
        )
        data = {}

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    data[source] = {
        "last_updated_at": last_updated_at,
        "updated_at": now_utc,
    }

    s3_client.put_object(
        Bucket=config.MINIO_BUCKET,
        Key=config.WATERMARK_KEY,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(
        "watermark_updated",
        extra={
            "source": source,
            "last_updated_at": last_updated_at,
            "updated_at": now_utc,
        },
    )