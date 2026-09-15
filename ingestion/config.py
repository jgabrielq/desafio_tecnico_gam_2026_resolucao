from dotenv import load_dotenv
import os

load_dotenv()

# Carrega as variáveis de ambiente de forma DRY para todos os outros módulos e aplica um Fallback.

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
