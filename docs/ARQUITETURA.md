# ARCHITECTURE.md — Decisões de Arquitetura

Documento de referência para o code review. Responde as 6 perguntas obrigatórias
do enunciado e registra todas as decisões arquiteturais tomadas ao longo do projeto.

---

## 1. Por que você particionou as tabelas desse jeito? Qual seria o impacto de particionar diferente?

### `silver.events` — `PARTITIONED BY (days(occurred_at))`

As três queries analíticas da Gold filtram por janela temporal:
- Query 1: últimos 30 dias por volume de eventos
- Query 2: tempo médio de resposta por mês
- Query 3: retenção por coorte (agrupada por mês de `signup_date`)

Particionar por `days(occurred_at)` permite que o Trino e o Spark façam
**partition pruning** — ao filtrar `occurred_at >= X`, o engine só abre os
arquivos das partições relevantes, ignorando todo o resto. Para as queries
acima, isso reduz o volume lido de "toda a tabela" para "só os dias do
intervalo filtrado".

**Por que não por `event_type`**: baixa cardinalidade (poucos tipos distintos)
gera partições muito desbalanceadas — alguns tipos têm ordens de magnitude mais
eventos que outros. Queries que não filtram por `event_type` ainda varreriam
tudo. Não há ganho real de pruning.

**Por que não por `customer_id`**: altíssima cardinalidade — com milhares de
clientes, isso geraria milhares de partições com pouquíssimos arquivos cada uma.
Esse é o problema clássico de **small files** no Iceberg: muitos arquivos
pequenos degradam tanto a performance de leitura (overhead de abertura de
arquivo por arquivo) quanto a de escrita (metadados de manifesto crescem
indefinidamente). Custo sem benefício.

**Impacto de particionar por `months(occurred_at)`**: geraria menos arquivos
(menos small files) mas perderia granularidade de pruning para a query dos
últimos 30 dias — o Trino teria que abrir pelo menos 2 partições mensais para
cobrir qualquer janela de 30 dias que cruze a virada do mês, em vez de abrir
exatamente os dias do intervalo.

### `silver.customers` — sem particionamento

Tabela de dimensão com poucos milhares de linhas (mesmo considerando o histórico
SCD2). Particionar uma dimensão tão pequena cria overhead de metadados e
gerenciamento de arquivos sem nenhum benefício de pruning — todas as queries
de customers vão ler a tabela inteira de qualquer forma.

### `bronze.events` / `bronze.customers` — sem particionamento

Bronze é append-only e o volume por `_batch_id` é pequeno. O particionamento
útil acontece na Silver, onde os dados estão deduplicados e com tipos corretos.
Adicionar particionamento na Bronze só geraria small files desnecessários.

---

## 2. Como você garantiu idempotência em cada etapa?

Idempotência significa: executar a mesma etapa duas vezes para o mesmo intervalo
de dados produz exatamente o mesmo resultado, sem duplicar nem perder dado.

### Raw zone (ingestão Python)

**Mecanismo**: nome de arquivo fixo e determinístico —
`raw/{source}/ingestion_date={date}/part-000.json.gz`. O `put_object` do boto3
sobrescreve automaticamente se o objeto já existir no MinIO. Rodar a ingestão
duas vezes para o mesmo `ingestion_date` substitui o arquivo anterior pelo
idêntico.

**Detalhe crítico descoberto em produção**: a API mock trata o parâmetro `since`
como **inclusivo** (`>=`), fazendo o registro de fronteira (cujo `updated_at`
igual ao watermark) retornar a cada execução. Sem tratamento, a segunda execução
do mesmo dia sobrescreveria o arquivo raw com apenas 1 registro (o de fronteira),
perdendo todos os demais. Solução: filtro client-side `updated_at > since_param`
após paginar tudo, antes de retornar os registros.

### Bronze (PySpark + Iceberg)

**Mecanismo**: `DELETE FROM bronze.{source} WHERE _batch_id = '{ingestion_date}'`
antes do `append`. O `_batch_id` é determinístico (igual à `ingestion_date`) —
nunca usa timestamp nem UUID. Rodar o job duas vezes apaga os registros da
primeira execução e reinsere os mesmos.

### Silver (PySpark + Iceberg)

**Mecanismo**: `MERGE INTO` com condição `WHEN MATCHED AND source.updated_at >
target.updated_at THEN UPDATE`. Registros já existentes na Silver com o mesmo
`event_id` e `updated_at` igual ou menor são **ignorados** — não duplicados, não
atualizados. Só registros genuinamente novos ou corrigidos (maior `updated_at`)
são processados.

Para `silver.customers` (SCD2): a condição de fechamento de versão exige
`updated_at` estritamente maior — reexecutar para o mesmo batch não fecha nem
duplica versões já registradas.

### Gold (PySpark + Iceberg)

**Mecanismo**: overwrite completo das tabelas Gold a cada execução. As tabelas
Gold são agregações pequenas derivadas da Silver — recalcular do zero é mais
simples e mais seguro do que tentar fazer merge de agregados. O resultado é
sempre determinístico dado o mesmo estado da Silver.

### Watermark

**Mecanismo**: o watermark só avança **depois** que a gravação no MinIO é
confirmada. Se a gravação falhar, o watermark permanece no valor anterior — a
próxima execução reprocessa o mesmo intervalo. Isso garante **at-least-once
delivery** com idempotência na raw (o arquivo sobrescreve).

---

## 3. Como você modelou a mudança de plano do cliente ao longo do tempo, e por quê?

### Decisão: SCD Tipo 2 (Slowly Changing Dimension Type 2)

`silver.customers` armazena o **histórico completo** de cada versão do cadastro
do cliente, com três colunas adicionais:

```
valid_from   TIMESTAMP   -- quando essa versão passou a valer (= updated_at do registro)
valid_to     TIMESTAMP   -- quando foi substituída (9999-12-31 se ainda vigente)
is_current   BOOLEAN     -- true para a versão ativa
```

Para responder "qual era o plano do cliente na data do evento":

```sql
SELECT c.plan
FROM silver.events e
JOIN silver.customers c
  ON e.customer_id = c.customer_id
 AND e.occurred_at BETWEEN c.valid_from AND c.valid_to
```

### Por que SCD2 e não as alternativas

**SCD Tipo 1 (sobrescrever)**: descartado — perderia o histórico de mudanças de
plano. Uma query sobre eventos antigos usaria o plano atual do cliente, não o
plano que ele tinha na data do evento. Isso geraria análises incorretas (ex:
"top clientes enterprise nos últimos 30 dias" incluiria clientes que só viraram
enterprise ontem).

**Snapshot por batch (uma linha por execução)**: descartado — geraria duplicatas
por cliente dentro do mesmo período sem mudança real, e exigiria lógica complexa
para encontrar a versão correta de uma data específica.

### `valid_to = 9999-12-31` como sentinela

Valor padrão da indústria para "sem data de expiração definida". Facilita queries
com `BETWEEN` sem precisar tratar `NULL` — `occurred_at BETWEEN valid_from AND
valid_to` funciona corretamente para versões ainda vigentes.

### Implementação com dois MERGEs separados

O Iceberg não suporta lógicas de matching diferentes num único `MERGE INTO`.
Por isso, dois MERGEs sequenciais:
1. **Fechar versões antigas**: `UPDATE SET valid_to, is_current = false` para
   registros que mudaram.
2. **Inserir versões novas**: `INSERT` das novas versões com `valid_from`,
   `valid_to = 9999-12-31`, `is_current = true`.

---

## 4. O que aconteceria com esse pipeline se o volume crescesse 100×? O que quebraria primeiro?

### O que quebraria primeiro: a ingestão da API

O rate limit de ~10 req/s é um gargalo linear. Com 100× mais eventos, haveria
100× mais páginas para paginar — o tempo de ingestão cresceria proporcionalmente,
podendo passar de minutos para horas. Em algum ponto, a janela de ingestão
ultrapassaria o intervalo entre execuções (diário), e o pipeline nunca terminaria.

### O que quebraria em segundo: a lista de dicts em memória

O `api_client.py` acumula todos os registros em memória antes de gravar no MinIO.
Com 10M+ registros, um dict por evento ocupa 1-2KB → 10-20GB em RAM antes de
qualquer escrita. O processo Python crasharia com OOM muito antes de chegar ao
`put_object`.

### Solução arquitetural para o volume 100×

**Para a ingestão**: substituir o script síncrono por arquitetura
producer/consumer com fila de mensagens (Kafka, AWS SQS ou equivalente):
- **Producer**: descobre as páginas disponíveis (uma chamada para `total_pages`)
  e publica uma mensagem por página na fila.
- **Consumers paralelos**: múltiplos workers consomem a fila, cada um buscando
  uma página diferente simultaneamente e gravando no MinIO.
- **Trade-offs**: retry granular por página (falha de um consumer não afeta
  outros), dead letter queue para páginas que falharam repetidamente, mas
  complexidade de setup significativamente maior.

**Para a memória**: streaming por página — em vez de acumular tudo, gravar cada
lote de N páginas no MinIO assim que chegam e descartar da memória, usando
arquivos `part-000.json.gz`, `part-001.json.gz`, etc.

**Para o Spark**: `spark.sql.shuffle.partitions` precisaria ser ajustado
(padrão 200 pode ser inadequado), e o particionamento da Silver por
`days(occurred_at)` continuaria correto — o volume maior por partição seria
tratado pelo Iceberg com mais arquivos por partição, não mais partições.

---

## 5. O problema de small files no Iceberg: ele existe na sua solução? Como você resolveria?

### Existe, especialmente na Bronze

A Bronze é append-only: cada execução diária gera um novo conjunto de arquivos
por `_batch_id`. Com execuções diárias de volume pequeno (como neste desafio),
cada `_batch_id` gera poucos arquivos — mas ao longo de meses/anos, o número
total de arquivos pequenos cresce indefinidamente. Leituras da Bronze precisam
abrir e processar metadados de todos esses arquivos, mesmo para queries simples.

A Silver é menos afetada porque o `MERGE INTO` reescreve as partições que tiveram
mudanças — o `merge-on-read` acumula delete files, mas o Iceberg os consolida
periodicamente.

### Como resolver

**`rewrite_data_files`**: compactação periódica — combina arquivos pequenos de
uma mesma partição em arquivos maiores, respeitando o `write.target-file-size-bytes`
configurado (padrão: 512MB). Pode ser agendado como uma task na DAG do Airflow,
rodando semanalmente ou quando o número de arquivos por partição ultrapassa um
limiar.

```python
from pyiceberg.catalog import load_catalog
catalog = load_catalog("lakehouse", ...)
table = catalog.load_table("bronze.events")
table.rewrite_data_files()
```

**`expire_snapshots`**: remove snapshots antigos do log de transações do Iceberg.
Cada operação (append, merge, delete) gera um snapshot — sem limpeza, o log
cresce indefinidamente e aumenta o tempo de planning das queries.

**`write.target-file-size-bytes`**: configurar na criação das tabelas para
orientar o Iceberg sobre o tamanho alvo de cada arquivo. Com volume pequeno,
reduzir esse valor para 128MB evita arquivos subpreenchidos.

---

## 6. O que você faria diferente com mais uma semana?

### Arquitetura de ingestão

Substituir o script síncrono por producer/consumer com fila de mensagens para
paralelizar a extração da API e eliminar o gargalo de memória (ver pergunta 4).

### Testes automatizados mais abrangentes

Adicionar testes unitários (pytest) das funções de transformação — especialmente
a lógica de deduplicação do `silver_events.py` e o SCD2 do `silver_customers.py`.
Testes que rodam sem depender de infraestrutura (usando DataFrames sintéticos
criados com `spark.createDataFrame`) permitiriam CI/CD real.

### dbt para a camada Gold

Substituir os jobs PySpark da Gold por modelos dbt (com adapter `dbt-trino` ou
`dbt-spark`). A Gold é essencialmente SQL declarativo — dbt entregaria
documentação automática, linhagem visual, testes de qualidade integrados e
versionamento de modelos SQL de forma muito mais elegante que PySpark.

### Data contracts entre camadas

Definir schemas explícitos e versionados para as interfaces entre camadas
(raw → bronze → silver → gold). Qualquer mudança de schema quebraria o contrato
e seria detectada antes de chegar em produção. Ferramentas como Great Expectations
ou Soda implementariam isso.

### Observabilidade com métricas

Adicionar métricas (não só logs) para monitorar a saúde do pipeline ao longo
do tempo: contagem de registros por execução comparada com a média histórica,
latência por etapa, taxa de eventos órfãos (`_customer_exists = false`). Um
dashboard Grafana sobre Prometheus ou similar permitiria detectar anomalias
silenciosas antes que virassem incidentes.

### Manutenção Iceberg automatizada

Tasks periódicas na DAG para `rewrite_data_files` e `expire_snapshots` em todas
as tabelas — não só quando o problema de small files se manifestar, mas de forma
proativa e agendada.

---

## Decisões adicionais registradas

### Watermark como JSON no MinIO (não tabela Iceberg nem Airflow Variable)

A etapa de ingestão é Python puro, sem Spark. Ler o watermark de uma tabela
Iceberg exigiria instanciar uma SparkSession no script de ingestão — overhead
desnecessário. O `boto3` já estava presente para gravar na raw zone, então
ler/escrever um arquivo JSON de controle no mesmo MinIO não adiciona nenhuma
dependência nova. É simples, leve, e auditável diretamente no console MinIO.

Alternativas descartadas:
- **Tabela Iceberg**: exigiria SparkSession no script Python de ingestão.
- **Airflow Variable**: acoplaria o pipeline ao orquestrador — o pipeline não
  funcionaria standalone sem o Airflow no ar.
- **Arquivo local**: não sobrevive a restart de container.
- **Tabela Postgres**: mistura controle de pipeline com fonte de dados.

### Eventos com `customer_id` órfão — manter, nunca descartar

Eventos com `customer_id` nulo ou inexistente na dimensão são preservados na
Silver com flag `_customer_exists = false`. Dado nunca é descartado
silenciosamente — a Gold e os checks de qualidade decidem o que fazer com eles.
Isso preserva auditabilidade e permite investigar a causa raiz da inconsistência.

### Gold — tabelas materializadas, não views

Views recalculariam do zero a cada query no Trino — para queries analíticas
complexas (coorte, tempo médio de resposta), isso seria lento e custoso. Tabelas
materializadas pré-calculam o resultado uma vez e servem múltiplas queries com
performance previsível. Adicionalmente, materializar permite adicionar
`_generated_at` como coluna de controle e auditoria.

### Definição de "últimos 30 dias" (query 1 da Gold)

Usa `MAX(occurred_at)` da Silver como referência, não `CURRENT_DATE`. O dataset
é sintético e fixo — usar `CURRENT_DATE` geraria zero resultados após a data
máxima dos dados. `MAX(occurred_at)` garante resultado determinístico
independente de quando o avaliador rodar a query.