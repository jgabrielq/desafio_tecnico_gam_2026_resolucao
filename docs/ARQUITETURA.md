Anotado. Consolidando tudo que já foi decidido e precisa entrar no ARCHITECTURE.md:

1. Particionamento (pergunta 1 do doc)

silver.events: PARTITIONED BY (days(occurred_at)) — justificativa: queries da Gold filtram por janela temporal, partition pruning agressivo. Por que não por event_type (baixa cardinalidade, desbalanceado) nem por customer_id (alta cardinalidade, small files).
silver.customers: sem particionamento — tabela pequena, dimensão.

2. Idempotência em cada etapa (pergunta 2)

Raw: nome de arquivo fixo part-000.json.gz → put_object sobrescreve.
Bronze: delete por _batch_id + append.
Silver: MERGE INTO por chave de negócio.
Gold: overwrite completo.

3. Watermark como JSON no MinIO (pergunta 2, complemento)

Ingestão é Python puro → não pode depender do Spark para ler controle de estado.
boto3 já presente → sem dependência nova.
Simples de auditar (legível no console MinIO).
Alternativa descartada: tabela Iceberg exigiria SparkSession no script Python.

4. Modelagem histórica do cliente — SCD Tipo 2 (pergunta 3)

valid_from, valid_to, is_current em silver.customers.
Permite responder "qual era o plano na data do evento" via join com occurred_at BETWEEN valid_from AND valid_to.

5. Volume 100× — o que quebraria primeiro (pergunta 4)

Ingestão da API: rate limit de 10 req/s vira gargalo linear.
Lista de dicts em memória: com 10M+ registros, RAM estouraria antes de gravar no MinIO.
Solução arquitetural alternativa: producer/consumer com fila de mensagens (Kafka/SQS) — consumers paralelos, retry granular por mensagem, dead letter queue. Trade-off: complexidade de setup vs. resiliência e throughput.

6. Small files no Iceberg (pergunta 5)

Existe na solução (ingestão diária de volume pequeno gera muitos arquivos pequenos na Bronze append-only).
Solução: rewrite_data_files periódico, write.target-file-size-bytes configurado.

7. O que faria diferente com mais uma semana (pergunta 6)

Arquitetura de fila para ingestão (producer/consumer).
Testes mais abrangentes (pytest nas transformações).
dbt para camada Gold.
Data contracts entre camadas.
Observabilidade com métricas (não só logs).