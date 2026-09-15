import sys
from pathlib import Path

# Adiciona a raiz do repositório ao PATH do Python
sys.path.append(str(Path(__file__).resolve().parent.parent))

import logging
from ingestion.api_client import fetch_all_events

# Configura o log básico para visualizar os eventos durante o teste
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def run_test():
    print("=== Testando busca de eventos na Mock API ===")
    
    # Executa a busca passando uma data inicial
    records, metrics = fetch_all_events(since="2026-03-01T00:00:00Z")

    print(f"\nTotal de registros retornados: {len(records)}")
    print("Métricas coletadas:", metrics)

    if records:
        print("\nExemplo do primeiro registro retornado:")
        print(records[0])

if __name__ == "__main__":
    run_test()