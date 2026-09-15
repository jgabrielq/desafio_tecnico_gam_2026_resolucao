# CONCEITOS_REVISAR.md — Guia de Revisão para o Code Review

Todos os conceitos técnicos aplicados no projeto, com explicação e onde cada
um aparece no código. Use como roteiro de estudo antes da conversa de 60 minutos.

---

## 1. Idempotência

**O que é**: uma operação é idempotente quando executá-la múltiplas vezes produz
o mesmo resultado que executá-la uma única vez. Em pipelines de dados, isso
significa: rodar duas vezes para o mesmo intervalo não duplica nem perde dado.

**Por que importa**: o Airflow reexecuta tasks em caso de falha (retries). Sem
idempotência, um retry duplicaria dados — um bug silencioso e difícil de detectar.

**Como foi aplicado no projeto**:
- **Raw**: nome de arquivo fixo `part-000.json.gz` → `put_object` sobrescreve.
- **Bronze**: `DELETE WHERE _batch_id = ingestion_date` antes do `append`.
- **Silver**: `MERGE INTO ... WHEN MATCHED AND updated_at > target.updated_at`.
- **Gold**: overwrite completo (tabelas pequenas, recalcular é seguro).
- **Watermark**: só avança após gravação confirmada.

**Pergunta que o avaliador pode fazer**: "O que aconteceria se o job da Bronze
falhasse depois do DELETE mas antes do append?" → O dado daquele batch seria
perdido na Bronze, mas a raw zone ainda teria o arquivo original — reprocessar
a Bronze para aquela `ingestion_date` recuperaria tudo.

---

## 2. Watermark / Ingestão Incremental

**O que é**: um marcador que registra "até onde já processei" em dados que chegam
continuamente. Na próxima execução, você só busca o que mudou depois desse ponto.

**Diferença entre watermark e data de execução**:
- `data_execucao` (`{{ ds }}` no Airflow): quando o pipeline rodou.
- `watermark` (`last_updated_at`): o timestamp mais recente dos dados já processados.

**Por que baseado em `updated_at` e não `occurred_at`**:
Late arrivals — eventos com `occurred_at` de 3 dias atrás chegam com
`updated_at = hoje`. Filtrar por `updated_at` captura esses eventos corretamente.
Filtrar por `occurred_at` os perderia.

**O bug descoberto em produção**: a API trata `since` como inclusivo (`>=`),
fazendo o registro de fronteira voltar a cada execução. Solução: filtro
client-side `updated_at > since_param` antes de retornar os registros.

**Onde está no código**: `ingestion/watermark.py` (leitura/escrita),
`ingestion/api_client.py` (filtro client-side), `ingestion/ingest.py`
(orquestração da atualização pós-gravação).

---

## 3. SCD Tipo 2 (Slowly Changing Dimension Type 2)

**O que é**: técnica de modelagem dimensional para preservar o histórico de
mudanças em dados que mudam ao longo do tempo. Em vez de sobrescrever o registro
(SCD Tipo 1), cada mudança gera uma nova linha com período de validade.

**Colunas adicionadas**:
- `valid_from`: quando essa versão começou a valer (`= updated_at` do registro).
- `valid_to`: quando foi substituída (`9999-12-31` se ainda vigente — sentinela).
- `is_current`: `true` para a versão ativa do cliente.

**Como usar em queries**:
```sql
-- Plano do cliente na data do evento
SELECT c.plan
FROM silver.events e
JOIN silver.customers c
  ON e.customer_id = c.customer_id
 AND e.occurred_at BETWEEN c.valid_from AND c.valid_to
```

**Por que não SCD Tipo 1** (sobrescrever): perderíamos o histórico — uma análise
de "clientes pro nos últimos 30 dias" incluiria clientes que só viraram pro ontem.

**Onde está no código**: `transform/silver_customers.py` — dois `MERGE INTO`
sequenciais (fechar versão antiga, inserir versão nova).

**Pergunta que o avaliador pode fazer**: "Quantos registros de histórico um
cliente que mudou de plano uma vez gera?" → 2 linhas: a original com
`valid_to = data_da_mudança`, e a nova com `valid_to = 9999-12-31`.

---

## 4. MERGE INTO (Upsert em Iceberg)

**O que é**: operação SQL que combina INSERT e UPDATE numa única instrução.
Compara registros de uma fonte com os de uma tabela alvo e decide o que fazer
com base em condições.

**Sintaxe usada no projeto**:
```sql
MERGE INTO silver.events AS target
USING silver_events_batch AS source
ON target.event_id = source.event_id
WHEN MATCHED AND source.updated_at > target.updated_at
    THEN UPDATE SET *
WHEN NOT MATCHED
    THEN INSERT *
```

**Por que `WHEN MATCHED AND updated_at > target.updated_at`**: garante que
registros corrigidos (mesmo `event_id` com `updated_at` maior) sobrescrevam a
versão anterior, mas registros duplicados com o mesmo `updated_at` sejam
ignorados — idempotência perfeita.

**`merge-on-read` vs `copy-on-write`**:
- `copy-on-write` (padrão): cada MERGE reescreve os arquivos Parquet inteiros
  das partições afetadas — escrita cara, leitura rápida.
- `merge-on-read` (usado no projeto): o MERGE acumula arquivos de delete/update
  separados dos dados originais — escrita rápida, leitura levemente mais cara
  (consolida na hora de ler). Melhor escolha quando há muitos MERGEs frequentes.

**Onde está no código**: `transform/silver_events.py` e
`transform/silver_customers.py`.

---

## 5. Apache Iceberg — conceitos fundamentais

**O que é**: um formato de tabela aberto (não um banco de dados) que adiciona
uma camada de metadados sobre arquivos Parquet no object storage (MinIO/S3).
Fornece: transações ACID, schema evolution, time travel, partition evolution.

**Estrutura de arquivos**:
```
s3://lakehouse/warehouse/
└── silver/
    └── events/
        ├── metadata/
        │   ├── v1.metadata.json   ← estado da tabela (schema, partições)
        │   ├── v2.metadata.json   ← após o primeiro MERGE
        │   └── snap-*.avro        ← manifests (lista de arquivos por snapshot)
        └── data/
            └── occurred_at_day=2026-03-11/
                ├── 00000-0-abc.parquet   ← dados reais
                └── 00001-0-def-deletes.parquet  ← delete file (merge-on-read)
```

**Por que Iceberg e não Delta Lake ou Hudi**:
- Delta Lake é proprietário da Databricks (embora open source, ecosistema mais
  fechado).
- Hudi é focado em streaming incremental — mais complexo para casos batch.
- Iceberg é o padrão aberto mais adotado em ambientes multi-engine (Spark +
  Trino lendo as mesmas tabelas sem conversão).

**Time travel**: cada operação gera um snapshot numerado. Você pode consultar
o estado da tabela em qualquer ponto anterior:
```sql
SELECT * FROM lakehouse.silver.events
FOR VERSION AS OF 3  -- snapshot número 3
```

**Schema evolution**: adicionar colunas novas não reescreve dados antigos —
os registros antigos recebem `NULL` na coluna nova. Foi o que permitiu absorver
o schema drift do batch 2 (`source_app`) sem quebrar a Bronze.

**Onde está no código**: todas as tabelas `lakehouse.bronze.*` e
`lakehouse.silver.*` são tabelas Iceberg. A criação usa `USING iceberg` no DDL.

---

## 6. Schema Drift

**O que é**: quando a estrutura (schema) dos dados muda sem aviso prévio —
um campo novo aparece no payload de uma API, uma coluna muda de tipo, etc.
Um pipeline frágil quebra. Um pipeline robusto absorve a mudança.

**Como se manifestou no projeto**: o batch 2 da API mock introduz um campo novo
`source_app` em todos os eventos. Sem tratamento, o `writeTo(...).append()`
da Bronze falhava com `INSERT_COLUMN_ARITY_MISMATCH`.

**Solução aplicada em duas partes**:
1. `write.spark.accept-any-schema=true` na tabela + `mergeSchema=true` no write:
   colunas novas são adicionadas automaticamente à tabela Iceberg.
2. Reordenação explícita das colunas antes do write:
   ```python
   known_columns = ["event_id", "customer_id", ...]
   drift_columns = sorted(c for c in df.columns if c not in known_columns)
   df = df.select(*known_columns, *drift_columns)
   ```
   O Iceberg exige que colunas conhecidas venham antes das novas — sem a
   reordenação, um segundo erro de ordering quebraria o job.

**Por que preservar `properties` como STRING na Bronze**: o campo `properties`
da API é um JSON semiestruturado com chaves que variam por `event_type`. Se
o Spark inferisse o schema, uma mudança nas chaves (schema drift dentro do
`properties`) quebraria a leitura. Como STRING, qualquer conteúdo é válido —
o parsing acontece na Silver ou na Gold, quando o schema já é conhecido.

**Onde está no código**: `transform/bronze_events.py` (tratamento do drift),
`AJUSTES_PARTE2_TRANSFORM.md` (causa raiz e evidências).

---

## 7. ACID em Lakehouse (Atomicidade, Consistência, Isolamento, Durabilidade)

**O que é**: conjunto de propriedades que garantem que operações no banco de
dados são confiáveis mesmo em caso de falha.

**Como o Iceberg implementa ACID sobre object storage**:
- **Atomicidade**: cada operação (append, merge, delete) cria um novo snapshot
  atomicamente. Se a operação falhar no meio, o snapshot incompleto não é
  registrado — a tabela permanece no estado anterior.
- **Consistência**: o catálogo REST (iceberg-rest) garante que só um writer
  por vez pode commitar um novo snapshot — evita conflitos de escrita concorrente.
- **Isolamento**: leitores veem sempre o último snapshot commitado — nunca veem
  um estado parcial de uma escrita em andamento.
- **Durabilidade**: os arquivos Parquet no MinIO são imutáveis após escritos.
  Um snapshot só referencia arquivos que já foram escritos com sucesso.

**Por que isso importa no projeto**: sem ACID, o `DELETE WHERE _batch_id = X`
seguido do `append` na Bronze poderia deixar a tabela num estado inconsistente
se o job falhasse entre as duas operações. Com Iceberg, o DELETE e o append
são operações separadas com snapshots separados — o pior caso é a tabela ficar
sem os dados do batch (recuperável reprocessando), nunca num estado corrompido.

---

## 8. Partition Pruning

**O que é**: otimização de query onde o engine (Spark ou Trino) identifica,
a partir dos filtros da query, quais partições físicas precisam ser lidas —
ignorando todas as outras. Resultado: menos I/O, queries mais rápidas.

**Como funciona no Iceberg**: os manifests do Iceberg armazenam estatísticas
por arquivo (min/max de valores, contagem de nulos). Ao planejar uma query com
`WHERE occurred_at >= '2026-02-01'`, o Trino consulta os manifests e descobre
quais partições `days(occurred_at)` têm dados nesse intervalo, sem abrir um
único arquivo Parquet.

**Como foi aplicado**: `silver.events` particionada por `days(occurred_at)` →
as queries da Gold que filtram por janela temporal leem apenas as partições
relevantes.

**Exemplo concreto**: query "últimos 30 dias" com 365 dias de dados → sem
pruning, lê 100% dos arquivos. Com pruning por `days(occurred_at)`, lê ~8%
(30/365 dias).

---

## 9. Small Files Problem

**O que é**: quando uma tabela tem muitos arquivos pequenos, o overhead de abrir
e processar metadados de cada arquivo cresce mais rápido que o volume de dados.
Queries ficam lentas não por volume de dados, mas por número de arquivos.

**Como se manifesta no projeto**: a Bronze append-only gera novos arquivos a
cada execução diária. Com execuções diárias por meses, centenas de arquivos
pequenos se acumulam. A Silver é menos afetada (MERGE consolida partições
afetadas), mas o `merge-on-read` acumula delete files que precisam ser
compactados periodicamente.

**Solução**: `rewrite_data_files` do Iceberg combina arquivos pequenos em
arquivos maiores dentro de cada partição. Deve ser agendado periodicamente
(ex: semanal) como task na DAG.

---

## 10. Late Arrival

**O que é**: eventos que chegam na fonte de dados com atraso — `occurred_at`
de 3 dias atrás mas `updated_at = hoje`. São comuns em sistemas distribuídos
onde eventos podem ser processados ou sincronizados com atraso.

**Por que é um problema**: um pipeline que ingere por `occurred_at` perderia
esses eventos — eles "pertencem" a uma janela temporal já processada. Um
pipeline que ingere por `updated_at` os captura corretamente, porque
`updated_at = hoje` está sempre dentro da janela atual.

**Como foi tratado no projeto**: o watermark é baseado em `updated_at`
(não `occurred_at`), garantindo que late arrivals sejam capturados
automaticamente na próxima execução após chegarem na API.

**Impacto na Silver**: um late arrival com `occurred_at` de 3 dias atrás vai
para a partição `days(occurred_at) = 3 dias atrás` na Silver — reescrevendo
essa partição via MERGE. Isso é correto e esperado.

---

## 11. Trino — Engine OLAP

**O que é**: engine de query SQL distribuída, otimizada para queries analíticas
(OLAP) sobre grandes volumes de dados. Diferente do Spark (processamento batch),
o Trino é stateless — cada query é independente, sem estado entre execuções.

**Como se conecta ao Iceberg no projeto**: o Trino tem um catálogo configurado
(`lakehouse`) que aponta para o mesmo catálogo REST do Iceberg que o Spark usa.
Tabelas criadas pelo Spark aparecem no Trino imediatamente — sem conversão,
sem sincronização manual.

**Diferença entre Trino e Spark para queries**:
- **Spark**: melhor para transformações complexas com estado (joins grandes,
  aggregations em múltiplos estágios, ML). Mais verboso para SQL puro.
- **Trino**: melhor para queries SQL analíticas interativas. Mais rápido para
  consultas ad-hoc sobre tabelas já materializadas. Não é adequado para
  transformações com muita lógica imperativa.

**Por que as queries da Gold rodam no Trino**: as 3 queries são SQL puro
(aggregations, joins, window functions) — o Trino as executa mais rapidamente
e com sintaxe mais limpa que o PySpark equivalente.

---

## 12. Window Functions em SQL

**O que é**: funções que calculam um valor para cada linha com base num conjunto
de linhas relacionadas (a "janela"), sem colapsar as linhas em grupos como o
`GROUP BY` faz.

**Usadas no projeto**:

```sql
-- Deduplicação na Silver (pegar o registro com maior updated_at por event_id)
ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY updated_at DESC)

-- Retenção por coorte (% de clientes ativos em cada mês após o signup)
COUNT(DISTINCT customer_id) OVER (PARTITION BY cohort_month)

-- Tempo médio de resposta (primeiro ticket_replied após ticket_opened)
MIN(occurred_at) OVER (PARTITION BY ticket_id, event_type)
```

**`PARTITION BY` vs `GROUP BY`**:
- `GROUP BY`: colapsa N linhas em 1 por grupo.
- `PARTITION BY` (em window function): mantém todas as N linhas, calcula o
  valor para cada uma com base nas demais do mesmo grupo.

---

## 13. Particionamento Hive

**O que é**: convenção de organização de arquivos em object storage onde o nome
do diretório codifica o valor de uma coluna:
`raw/events/ingestion_date=2026-03-11/part-000.json.gz`

**Por que é importante**: o Spark e o Trino reconhecem automaticamente esse
padrão e criam uma coluna virtual `ingestion_date` a partir do nome do diretório
— sem que o valor precise estar dentro do arquivo. Queries com filtro
`WHERE ingestion_date = '2026-03-11'` leem apenas o diretório correspondente.

**Onde foi aplicado**: raw zone (`raw/{source}/ingestion_date={date}/`).
As tabelas Iceberg têm seu próprio mecanismo de particionamento (via manifests),
mas o particionamento Hive na raw zone facilita a leitura incremental por
`ingestion_date` nos jobs de Bronze.

---

## 14. `at-least-once` vs `exactly-once`

**`at-least-once`**: cada registro é processado pelo menos uma vez. Pode haver
reprocessamento (duplicatas temporárias), mas nada é perdido. Mais simples de
implementar.

**`exactly-once`**: cada registro é processado exatamente uma vez. Muito mais
complexo — exige coordenação entre o produtor e o consumidor para garantir que
um retry não reprocesse o mesmo dado.

**Como o projeto implementa `at-least-once` com idempotência**:
O pipeline garante `at-least-once` (watermark só avança após confirmação) e
usa idempotência para que o reprocessamento não gere duplicatas — o efeito
prático é equivalente a `exactly-once`, sem a complexidade de uma implementação
formal.

**Onde está no código**: a ordem do `ingest.py` — `write_raw → write_watermark`
(nunca o contrário) garante que, em caso de falha, o watermark não avança e
o mesmo dado é reprocessado na próxima execução.

---

## Perguntas prováveis do code review — respostas em uma frase

1. **"Por que o watermark está no MinIO e não numa tabela Iceberg?"**
   → A ingestão é Python puro; criar uma SparkSession só para ler um arquivo de
   controle seria overhead desnecessário — boto3 já estava presente.

2. **"Como você garante que rodar duas vezes não duplica dados?"**
   → Raw: overwrite pelo nome fixo do arquivo. Bronze: DELETE + append por
   `_batch_id`. Silver: MERGE com condição `updated_at >`. Gold: overwrite total.

3. **"O que é SCD2 e por que você usou?"**
   → Técnica que preserva o histórico de mudanças — cada versão do registro tem
   `valid_from`/`valid_to`. Permite saber qual era o plano do cliente na data
   exata de um evento, via `BETWEEN valid_from AND valid_to`.

4. **"Por que `properties` é STRING e não struct?"**
   → O schema de `properties` varia por `event_type` e pode ter schema drift —
   como STRING, qualquer conteúdo é válido e o parsing acontece downstream.

5. **"O que é merge-on-read e por que você usou?"**
   → O MERGE acumula arquivos de delete/update separados em vez de reescrever
   partições — escrita mais rápida, levemente mais lenta na leitura. Melhor
   quando há MERGEs frequentes como no nosso caso.

6. **"Como você trataria um evento com `customer_id` que não existe na dimensão?"**
   → Preservado na Silver com `_customer_exists = false` — dado nunca é
   descartado silenciosamente; a Gold e os checks de qualidade decidem o que
   fazer.

7. **"O que quebraria primeiro com 100x mais volume?"**
   → A ingestão da API: rate limit de 10 req/s + lista de dicts em memória
   estourariam antes de qualquer outra coisa.

8. **"Como resolveria o problema de small files?"**
   → `rewrite_data_files` periódico agendado na DAG, com `write.target-file-size-bytes`
   configurado na criação das tabelas.