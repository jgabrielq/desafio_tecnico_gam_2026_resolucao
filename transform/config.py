import os
from dotenv import load_dotenv

load_dotenv()

# MinIO
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",      "lakehouse")

# Catálogo Iceberg (já configurado no ambiente)
ICEBERG_CATALOG  = "lakehouse"

# Namespaces
BRONZE_NAMESPACE = f"{ICEBERG_CATALOG}.bronze"
SILVER_NAMESPACE = f"{ICEBERG_CATALOG}.silver"

# Prefixos raw zone
RAW_EVENTS_PREFIX    = "raw/events"
RAW_CUSTOMERS_PREFIX = "raw/customers"
