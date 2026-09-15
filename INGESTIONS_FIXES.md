Instruções para o Claude Code — correções cirúrgicas em ingest.py e storage.py
Preciso que você faça correções cirúrgicas em dois arquivos: ingestion/ingest.py e
ingestion/storage.py. Leia apenas esses dois arquivos antes de fazer qualquer alteração.
Não mexa em nenhum outro arquivo do repositório.
Correções em storage.py

1. Adicionar import de gzip no topo do arquivo:

python
import gzip

2. Alterar a assinatura de save_raw_batch para receber ingestion_date em vez de batch_id:

python
def save_raw_batch(s3_client, records: list[dict], source: str, ingestion_date: str) -> str:

3. Alterar o object_key para seguir o padrão Hive com ingestion_date e nome fixo part-000.json.gz:

python
object_key = f"raw/{source}/ingestion_date={ingestion_date}/part-000.json.gz"

4. Comprimir o conteúdo com gzip antes de enviar ao MinIO:

python
body = gzip.compress(ndjson_content.encode("utf-8"))

5. Atualizar o put_object para usar body comprimido e ContentType correto:

python
s3_client.put_object(
    Bucket=config.MINIO_BUCKET,
    Key=object_key,
    Body=body,
    ContentType="application/gzip",
)

6. Remover o parâmetro batch_id da função auxiliar _to_ndjson — ela só precisa de records:

python
def _to_ndjson(records: list[dict]) -> str:

E ajustar a chamada interna correspondente.

Correções em ingest.py

1. Adicionar import de sys no topo:

python
import sys

2. Alterar a assinatura de run_pipeline para receber ingestion_date:

python
def run_pipeline(ingestion_date: str) -> None:

3. Remover a linha que gera batch_id por timestamp — não é mais necessária.

4. Nas duas chamadas a save_raw_batch (uma para events, outra para customers), substituir batch_id=batch_id por ingestion_date=ingestion_date:

python
save_raw_batch(s3_client, records=events, source="events", ingestion_date=ingestion_date)
save_raw_batch(s3_client, records=customers, source="customers", ingestion_date=ingestion_date)

5. Atualizar o bloco if __name__ == "__main__" para aceitar a data via linha de comando:

python
if __name__ == "__main__":
    from datetime import date
    ingestion_date = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    run_pipeline(ingestion_date)

6. Atualizar as mensagens de log para incluir ingestion_date em vez de batch_id:

python
logger.info(f"Iniciando pipeline de ingestão | ingestion_date: {ingestion_date}")
logger.info(f"Pipeline finalizado com sucesso | ingestion_date: {ingestion_date}")

7. Adicionar log final estruturado antes do log de finalização:

python
logger.info("ingestion_completed", extra={
    "ingestion_date": ingestion_date,
    "events_records": len(events) if events else 0,
    "customers_records": len(customers) if customers else 0,
})

Após as alterações, não rode nenhum teste — apenas salve os arquivos. 
O desenvolvedor vai validar manualmente.