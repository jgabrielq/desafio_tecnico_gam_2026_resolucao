import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from ingestion import config

logger = logging.getLogger(__name__)


def fetch_customers(since: str | None = None) -> list[dict]:
    """
    Extrai clientes do Postgres com updated_at > since.
    Retorna lista de dicionários com datas formatadas em ISO 8601 / YYYY-MM-DD.
    """
    since_param = since if since else "1970-01-01T00:00:00Z"
    conn = None

    # Cast explícito no SQL para garantir tipos de data serializáveis em JSON
    query = """
        SELECT customer_id, company_name, plan, segment,
               signup_date::text, country, is_active,
               updated_at AT TIME ZONE 'UTC' AS updated_at
        FROM crm.customers
        WHERE updated_at > %(since)s
        ORDER BY updated_at ASC
    """

    try:
        conn = psycopg2.connect(
            host=config.POSTGRES_HOST,
            port=config.POSTGRES_PORT,
            dbname=config.POSTGRES_DB,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD,
        )

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, {"since": since_param})
            rows = cursor.fetchall()

            # Converte objetos ISO de timestamp retornados pelo Postgres para string
            records = []
            for row in rows:
                record = dict(row)
                if record.get("updated_at"):
                    record["updated_at"] = record["updated_at"].strftime("%Y-%m-%dT%H:%M:%SZ")
                records.append(record)

        logger.info(
            "postgres_fetch_completed",
            extra={
                "source": "customers",
                "since": since_param,
                "records_fetched": len(records),
            },
        )
        return records

    except Exception as e:
        logger.error(f"Erro ao extrair clientes do PostgreSQL: {e}")
        raise
    finally:
        if conn:
            conn.close()