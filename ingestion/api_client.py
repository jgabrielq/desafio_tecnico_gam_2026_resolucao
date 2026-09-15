import logging
import time
import requests
from ingestion import config

logger = logging.getLogger(__name__)


def _fetch_page_with_retry(url: str, headers: dict, params: dict, metrics: dict) -> dict:
    """
    Realiza a requisição de uma única página aplicando regras de retry/backoff.
    - HTTP 429: Lê 'Retry-After' (ou fallback 60s) e aguarda.
    - HTTP 503/5xx: Aplica backoff exponencial (1s, 2s, 4s, 8s, 16s) até 5 tentativas.
    - HTTP 4xx: Erro de cliente, interrompe imediatamente.
    """
    MAX_ATTEMPTS = 5
    attempt = 0

    while attempt < MAX_ATTEMPTS:
        attempt += 1
        try:
            response = requests.get(
                url, headers=headers, params=params, timeout=10
            )

            # 1. Tratamento de Rate Limit (HTTP 429)
            if response.status_code == 429:
                metrics["retries_429"] += 1
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(
                    f"Rate limit (429) atingido na página {params['page']}. "
                    f"Aguardando {retry_after}s..."
                )
                time.sleep(retry_after)
                continue

            # 2. Tratamento de falhas do servidor (HTTP 5xx / 503)
            if response.status_code >= 500:
                metrics["retries_503"] += 1
                if attempt < MAX_ATTEMPTS:
                    wait_time = 2 ** (attempt - 1)  # 1s, 2s, 4s, 8s, 16s
                    logger.warning(
                        f"Erro {response.status_code} na página {params['page']}. "
                        f"Tentativa {attempt}/{MAX_ATTEMPTS}. Aguardando {wait_time}s..."
                    )
                    time.sleep(wait_time)
                    
                    continue
                
                else:
                    response.raise_for_status()

            # Erros 4xx interrompem a execução sem tentar novamente
            response.raise_for_status()
            
            return response.json()

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as e:
            metrics["retries_503"] += 1
            if attempt < MAX_ATTEMPTS:
                wait_time = 2 ** (attempt - 1)
                logger.warning(
                    f"Erro de conexão na página {params['page']}: {e}. "
                    f"Tentativa {attempt}/{MAX_ATTEMPTS}. Aguardando {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                raise

    raise RuntimeError(
        f"Falha ao buscar página {params['page']} após {MAX_ATTEMPTS} tentativas."
    )


def fetch_all_events(since: str | None = None) -> tuple[list[dict], dict]:
    """
    Busca todos os eventos na API com updated_at >= since.
    Trata paginação, rate limit (HTTP 429) e falhas intermitentes (HTTP 503).
    Retorna uma tupla contendo (lista de registros brutos, dicionário de métricas).
    """
    start_time = time.time()
    since_param = since if since else "1970-01-01T00:00:00Z" # Fallback para buscar os primeiros dados

    all_records: list[dict] = []
    page = 1
    total_pages = 1 # Valor arbitrário que será substituído pelo valor correto após a chamada á API

    metrics = {
        "pages_read": 0,
        "retries_429": 0,
        "retries_503": 0,
        "records_fetched": 0,
        "duration_seconds": 0.0,
    }

    headers = {
        "X-API-Key": config.API_KEY,
        "Accept": "application/json",
    }

    while page <= total_pages:
        url = f"{config.API_BASE_URL}/events"
        params = {
            "since": since_param,
            "page": page,
            "page_size": config.API_PAGE_SIZE,
        }

        # Coletando os dados:
        response_data = _fetch_page_with_retry(url, headers, params, metrics)

        page_records = response_data.get("data", []) # Registros principais ('records'). É isso que a gente quer!
        all_records.extend(page_records) # Junta eles na lista que contém o que já foi coletado

        total_pages = response_data.get("total_pages", total_pages)
        
        # Atualizando as métricas para o próximo request
        metrics["pages_read"] += 1
        page += 1

        # Throttle preventivo para não estourar o limite da API (manter o ritmo < 10 req/s)
        time.sleep(0.1)

    # A API trata 'since' como inclusivo (>=), então o registro de fronteira
    # (updated_at == since_param) volta a cada execução. Filtra client-side
    # para manter a semântica de incremental estritamente exclusiva e evitar
    # reprocessar o mesmo registro em execuções consecutivas no mesmo dia.
    all_records = [r for r in all_records if r.get("updated_at", "") > since_param]

    metrics["records_fetched"] = len(all_records)
    metrics["duration_seconds"] = round(time.time() - start_time, 2)

    logger.info(
        "api_fetch_completed",
        extra={
            "source": "events",
            "since": since_param,
            **metrics,
        },
    )

    return all_records, metrics


