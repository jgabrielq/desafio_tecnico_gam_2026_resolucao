Job transform/gold.py: três funções, uma por tabela Gold — top_clientes_30d, tempo_resposta_ticket, retencao_coorte. Cada uma faz DELETE WHERE 1=1 + INSERT (overwrite completo, idempotente). O job não recebe ingestion_date — recalcula sempre do estado completo da Silver.

Três queries SQL em sql/: versões simples que só leem as tabelas Gold já materializadas, mais eficientes para o avaliador rodar. Cada uma tem uma orientação de interpretação (o parágrafo que o enunciado exige).

Validação incluída: comandos para confirmar contagens, testar idempotência, e exportar os resultados para CSV diretamente via Trino CLI.