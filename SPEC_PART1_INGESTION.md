# SPEC — Parte 1: Ingestão (raw zone)

Especificação técnica completa para implementação do script de ingestão.
Leia este arquivo inteiro antes de escrever qualquer código.

---

## Estrutura de arquivos a criar

```
ingestion/
├── __init__.py
├── config.py           # constantes e configurações via variáveis de ambiente
├── watermark.py        # leitura e escrita do watermark no MinIO
├── api_client.py       # cliente da API com retry/backoff
├── postgres_client.py  # extração incremental do Postgres
├── storage.py          # gravação na raw zone (MinIO)
└── ingest.py           # orquestrador — ponto de entrada CLI
```

---

## `config.py`

Todas as configurações via `os.getenv()`. Usar `python-dotenv` para carregar o `.env`
do repositório raiz. Não hardcodar nenhum valor — apenas o fallback local de desenvolvimento
é aceitável como segundo argumento do `getenv`.

```python
from dotenv import load_dotenv
import os

load_dotenv()

# MinIO
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",      "lakehouse")

# API mock
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_KEY      = os.getenv("API_KEY",      "desafio-2026")
API_PAGE_SIZE = int(os.getenv("API_PAGE_SIZE", "500"))

# PostgreSQL
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "crm")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "app")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "app")

# Caminhos no MinIO
WATERMARK_KEY        = "raw/control/watermarks.json"
RAW_EVENTS_PREFIX    = "raw/events"
RAW_CUSTOMERS_PREFIX = "raw/customers"
```

---

## `watermark.py`

### Responsabilidade
Ler e escrever o arquivo `raw/control/watermarks.json` no MinIO.
Esse arquivo é o mecanismo de ingestão incremental: registra o maior `updated_at`
já processado por cada fonte, para que a próxima execução busque apenas o que mudou.

### Estrutura do JSON
```json
{
  "events": {
    "last_updated_at": "2026-03-11T14:02:11Z",
    "updated_at": "2026-03-11T14:05:33Z"
  },
  "customers": {
    "last_updated_at": "2026-03-10T09:00:00Z",
    "updated_at": "2026-03-11T14:05:33Z"
  }
}
```
- `last_updated_at`: maior `updated_at` visto nos dados. Usado como `since=` na próxima chamada.
- `updated_at`: timestamp de quando o watermark foi registrado (auditoria).

### Funções

```python
def read_watermark(s3_client, source: str) -> str | None:
    """
    Lê o watermark de uma fonte ('events' ou 'customers') do MinIO.
    Retorna None se o arquivo não existir (primeira execução) ou se
    a chave da fonte não existir no JSON.
    Nunca lança exceção por ausência do arquivo — trata como primeira execução.
    """

def write_watermark(s3_client, source: str, last_updated_at: str) -> None:
    """
    Atualiza o watermark de uma fonte específica.
    Fluxo: lê o JSON atual (ou {} se não existir) → atualiza só a chave
    da fonte → reescreve o arquivo inteiro.
    Isso preserva watermarks de outras fontes.
    Campo 'updated_at' deve ser o datetime.utcnow() em ISO 8601 com sufixo Z.
    """
```

### Comportamento na primeira execução
`read_watermark` retorna `None` → o chamador usa `"1970-01-01T00:00:00Z"` como
fallback (busca tudo desde o início).

---

## `api_client.py`

### Responsabilidade
Paginar a API `/events`, tratar erros HTTP, respeitar rate limit, e retornar
todos os registros brutos como lista de dicts.

### Função principal

```python
def fetch_all_events(since: str) -> tuple[list[dict], dict]:
    """
    Busca todos os eventos com updated_at >= since.
    Retorna: (lista de registros brutos, dict de métricas)
    Métricas: {"pages_read": int, "retries_429": int, "retries_503": int,
               "records_fetched": int, "duration_seconds": float}
    """
```

### Tratamento de status HTTP

| Status | Comportamento |
|---|---|
| 200 | Processa normalmente |
| 429 | Lê header `Retry-After` (fallback: 60s). Espera e retenta. Incrementa `retries_429`. |
| 503 | Backoff exponencial: 1s → 2s → 4s → 8s → 16s. Máx 5 tentativas. Lança exceção após esgotar. Incrementa `retries_503`. |
| Outros 5xx | Mesmo tratamento do 503. |
| Outros 4xx | Lança exceção imediatamente — erro de cliente, não retenta. |

### Throttle preventivo
Inserir `time.sleep(0.1)` entre requisições de página para manter < 10 req/s.

### Paginação
Loop `while page <= total_pages`. O `total_pages` vem do primeiro response.
Usar `?since={since}&page={page}&page_size={page_size}` como query params.
Header obrigatório: `X-API-Key: {API_KEY}`.

### Log estruturado ao final
```python
logger.info("api_fetch_completed", extra={
    "source": "events",
    "since": since,
    "records_fetched": <int>,
    "pages_read": <int>,
    "retries_429": <int>,
    "retries_503": <int>,
    "duration_seconds": <float>
})
```

---

## `postgres_client.py`

### Responsabilidade
Extrair clientes do Postgres incrementalmente, usando `updated_at > since`.

### Função principal

```python
def fetch_customers(since: str) -> list[dict]:
    """
    Extrai clientes com updated_at > since.
    Retorna lista de dicts com todos os campos.
    updated_at retornado como string ISO 8601 (não objeto datetime).
    signup_date retornado como string YYYY-MM-DD.
    """
```

### Query — usar parâmetros nomeados (NUNCA f-string ou .format() com valores externos)
```sql
SELECT customer_id, company_name, plan, segment,
       signup_date::text, country, is_active,
       updated_at AT TIME ZONE 'UTC' AS updated_at
FROM crm.customers
WHERE updated_at > %(since)s
ORDER BY updated_at ASC
```

### Conexão
Usar `psycopg2`. Fechar conexão explicitamente em bloco `try/finally`.
Não usar pool de conexões — extração é pontual, uma única query.

---

## `storage.py`

### Responsabilidade
Serializar registros como NDJSON comprimido (gzip) e gravar no MinIO.

### Função principal

```python
def write_raw(s3_client, records: list[dict],
              source: str, ingestion_date: str) -> str:
    """
    Serializa records como NDJSON gzip e grava no MinIO.
    Retorna o S3 key completo do arquivo gravado.

    Caminho: raw/{source}/ingestion_date={ingestion_date}/part-000.json.gz
    Idempotência: put_object sobrescreve se o key já existir.
    """
```

### Decisões técnicas — implementar exatamente assim

- **NDJSON**: uma linha JSON por registro, separadas por `\n`. NÃO usar array JSON `[...]`.
  Isso facilita leitura linha a linha pelo Spark e permite append eficiente.
- **gzip**: comprimir com `gzip.compress()` antes de enviar ao MinIO.
- **Nome de arquivo fixo**: `part-000.json.gz` — determinístico, garante overwrite na idempotência.
- **Sem transformação**: gravar o dict exatamente como veio da fonte.
  `properties` (campo da API) deve ser serializado como string JSON aninhada, não como dict.
  Usar `json.dumps(record, default=str)` para lidar com tipos não serializáveis (datetime, date).
- **Content-Type**: `application/gzip` no `put_object`.

### Função auxiliar de serialização

```python
def _serialize_ndjson_gz(records: list[dict]) -> bytes:
    """Converte lista de dicts para bytes NDJSON gzip."""
```

---

## `ingest.py`

### Responsabilidade
Orquestrador: inicializa clientes, coordena as etapas na ordem correta,
atualiza watermarks apenas após gravação confirmada.

### Ordem de execução — crítica, não alterar

```
1. Inicializar s3_client (boto3), logger
2. Ler watermark de 'events'   → since_events
3. Ler watermark de 'customers' → since_customers
4. fetch_all_events(since_events)    → events_records, api_metrics
5. fetch_customers(since_customers)  → customers_records
6. write_raw(events_records,   'events',    ingestion_date)
7. write_raw(customers_records, 'customers', ingestion_date)
8. write_watermark('events',    max(updated_at) dos events_records)
9. write_watermark('customers', max(updated_at) dos customers_records)
10. Log final com totais de todas as fontes
```

**Regra de ouro**: watermarks (passos 8 e 9) só são atualizados APÓS
a gravação no MinIO (passos 6 e 7) ser confirmada. Se a gravação falhar,
o watermark não avança — a próxima execução reprocessa o mesmo intervalo.

### Tratamento de lista vazia
Se `events_records` ou `customers_records` vier vazio:
- Não gravar arquivo na raw (não criar arquivo vazio).
- Não atualizar o watermark dessa fonte.
- Logar um warning: `"no_records_found"` com a fonte e o `since` usado.

### Ponto de entrada CLI

```python
if __name__ == "__main__":
    import sys
    from datetime import date
    ingestion_date = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    run_ingestion(ingestion_date)
```

Uso: `python ingestion/ingest.py 2026-03-11`

### Log final estruturado

```python
logger.info("ingestion_completed", extra={
    "ingestion_date": ingestion_date,
    "events": {"records": <int>, "file": <s3_key ou None>},
    "customers": {"records": <int>, "file": <s3_key ou None>},
    "duration_seconds": <float>
})
```

---

## Configuração de logging

Usar `logging` padrão com formato JSON-like no extra. Configurar no `ingest.py`:

```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)
```

Cada módulo deve ter seu próprio `logger = logging.getLogger(__name__)`.

---

## Dependências Python a instalar

```
boto3          # cliente S3/MinIO
requests       # chamadas HTTP à API
psycopg2-binary # conexão Postgres
python-dotenv  # carrega .env
```

Se o repositório já tem `requirements.txt` ou `pyproject.toml`, adicionar lá.
Caso contrário, criar `requirements.txt` na raiz do seu repositório de solução.

---

## Validação após implementação

Rodar na sequência e confirmar cada passo:

```bash
# 1. Primeira execução (busca tudo desde 1970)
python ingestion/ingest.py 2026-03-11

# 2. Confirmar arquivos no MinIO
# Console: http://localhost:9001
# Deve existir:
#   lakehouse/raw/events/ingestion_date=2026-03-11/part-000.json.gz
#   lakehouse/raw/customers/ingestion_date=2026-03-11/part-000.json.gz
#   lakehouse/raw/control/watermarks.json

# 3. Confirmar watermark avançou
# Abrir watermarks.json no console MinIO
# last_updated_at deve ser um timestamp recente, não 1970

# 4. Segunda execução — TESTE DE IDEMPOTÊNCIA
python ingestion/ingest.py 2026-03-11
# Deve sobrescrever os mesmos arquivos, não criar novos
# Contagem de objetos no MinIO = igual à do passo 2

# 5. Confirmar logs estruturados no terminal
# Deve aparecer: api_fetch_completed, ingestion_completed com métricas
```

---

## Notas para o code review

- O avaliador vai rodar `pipeline → pipeline de novo` e verificar que nenhum dado foi duplicado.
- A idempotência da raw é garantida pelo nome de arquivo fixo (`part-000.json.gz`) que o `put_object` sobrescreve.
- A idempotência do watermark é garantida pela ordem: só avança após gravação confirmada.
- O campo `properties` da API deve chegar na raw como string JSON (não dict) para preservar o payload exatamente como veio.
- Registros com `customer_id` nulo devem ser preservados na raw — nenhum filtro aqui.
- Schema drift (campo novo na API): como gravamos JSON sem schema fixo, campos novos chegam automaticamente na raw sem quebrar nada.