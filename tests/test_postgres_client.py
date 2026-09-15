import sys
import logging
from ingestion.postgres_client import fetch_customers

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def run_test():
    print("=== Testando extração de clientes do PostgreSQL ===")
    
    # 1. Carga inicial (desde a data padrão)
    records = fetch_customers(since="1970-01-01T00:00:00Z")
    print(f"\nTotal de clientes retornados: {len(records)}")
    
    if records:
        print("\nExemplo do primeiro registro retornado:")
        print(records[0])
        
        # 2. Teste da lógica incremental usando o maior updated_at extraído
        last_updated = records[-1]["updated_at"]
        print(f"\n=== Testando busca incremental com 'since' = {last_updated} ===")
        incremental_records = fetch_customers(since=last_updated)
        print(f"Total de novos registros retornados (esperado 0): {len(incremental_records)}")

if __name__ == "__main__":
    run_test()