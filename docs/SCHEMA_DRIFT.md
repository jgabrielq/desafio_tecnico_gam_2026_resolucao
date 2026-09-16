# SCHEMA_DRIFT.md — Entendendo Schema Drift no Projeto

Documento de referência completo sobre schema drift: o que é, como se manifestou
no projeto, como foi tratado, e alternativas com trade-offs. Use como material de
estudo antes do code review.

---

## O que é Schema Drift

Schema drift é quando a **estrutura dos dados muda sem aviso prévio**. Em
pipelines de dados, isso acontece quando uma fonte externa (API, banco, arquivo)
começa a entregar dados com estrutura diferente da que o pipeline foi construído
para receber.

As formas mais comuns:

| Tipo | Exemplo |
|---|---|
| Campo novo | API passa a retornar `source_app` em todos os eventos |
| Campo removido | API para de retornar `channel` |
| Campo renomeado | `customer_id` vira `client_id` |
| Tipo alterado | `plan` era string, passa a ser inteiro |
| Schema interno variável | `properties` tem chaves diferentes por `event_type` |

Um pipeline frágil **quebra** diante de qualquer um desses casos. Um pipeline
robusto **absorve** a mudança — ou a detecta e alerta, dependendo da estratégia.

---

## Como o Schema Drift chegou neste projeto

O enunciado do desafio documenta isso como **comportamento intencional**:
*"a partir de determinada data, um campo novo aparece no payload"*. No batch 2,
o campo `source_app` passou a chegar em todos os eventos da API.

O caminho percorrido pelo drift até o pipeline:

```
API mock (batch 2)
  → retorna JSON com campo novo: { ..., "source_app": "mobile" }
  → api_client.py coleta e grava na raw (NDJSON sem schema — ok, passa)
  → bronze_events.py tenta append na tabela Iceberg
  → FALHA: tabela tem N colunas, DataFrame tem N+1
```

A **raw zone absorveu o drift sem problema** — NDJSON é só texto, não tem
schema. O problema só apareceu quando o Spark tentou escrever numa tabela
Iceberg com schema fixo declarado no `CREATE TABLE`.

---

## Dois casos distintos no projeto — não confundir

O projeto teve **dois problemas de schema**, com naturezas completamente
diferentes. É importante distingui-los para o code review.

### Caso 1 — `source_app`: campo novo no payload (drift clássico)

**Quando apareceu**: batch 2 (campo inexistente no batch 1).

**O problema**: o Iceberg, por padrão, é schema-strict na escrita. Se o
DataFrame tem mais colunas do que a tabela declarada, ele rejeita:

```
AnalysisException: INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS
Cannot write to `lakehouse`.`bronze`.`events` — too many data columns
```

**Onde acontece**: na **escrita** na tabela Iceberg (`writeTo(...).append()`).

**Solução**: habilitar schema evolution — ver seção "Como tratamos" abaixo.

---

### Caso 2 — `properties`: schema interno variável (sempre existiu)

**Quando apareceu**: batch 1 (campo presente desde sempre).

**O problema**: diferente do `source_app`, o `properties` **sempre existiu**.
O problema era que o Spark, ao ler o JSON, **inferia `properties` como um
`struct` aninhado** — expandia as chaves do JSON em subcolunas tipadas:

```
# O que o Spark inferiu (errado para nosso caso):
properties: struct<ticket_id: string, priority: string, agent_id: string>

# O que queríamos (correto):
properties: string  ← payload preservado exatamente como veio
```

Se mantivéssemos o struct, dois problemas apareceriam:

1. **Perda de dados**: um evento de `event_type = 'page_view'` tem
   `properties: {page_url: string, duration: int}` — campos completamente
   diferentes do evento `ticket_opened`. O schema inferido do primeiro batch
   não cobriria os campos do segundo.

2. **Drift interno**: qualquer chave nova dentro do `properties` quebraria
   o schema do struct inferido anteriormente.

**Onde acontece**: na **leitura** do JSON (`spark.read.json()`), antes de
qualquer escrita.

**Solução**: converter para STRING antes de gravar:

```python
from pyspark.sql import functions as F
df = df_raw.withColumn("properties", F.to_json(F.col("properties")))
```

`to_json()` pega o struct inferido e serializa de volta para uma string JSON
— preservando o payload exatamente como veio, sem perda de campos, e sem
criar dependência do schema interno do `properties`.

---

### Tabela comparativa

| | `properties` | `source_app` |
|---|---|---|
| Quando apareceu | Batch 1 (sempre existiu) | Batch 2 (campo novo) |
| Tipo do problema | Spark inferiu struct em vez de string | Coluna nova rejeitada pelo Iceberg |
| Solução | `to_json()` na leitura | `mergeSchema=true` + reordenação na escrita |
| Onde acontece | Na leitura do JSON | Na escrita na tabela Iceberg |
| Impacto sem tratamento | Perda de dados em eventos futuros | Job falha com exceção |

---

## Como tratamos o `source_app` (drift clássico) — dois passos obrigatórios

A solução exigiu **dois passos independentes** — e entender por que dois e não
um é fundamental para o code review.

### Passo 1 — Habilitar schema evolution na tabela Iceberg

```python
spark.sql(f"""
    ALTER TABLE {config.BRONZE_NAMESPACE}.events
    SET TBLPROPERTIES ('write.spark.accept-any-schema' = 'true')
""")

df.writeTo(f"{config.BRONZE_NAMESPACE}.events") \
  .option("mergeSchema", "true") \
  .append()
```

Isso diz ao Iceberg: *"aceite colunas novas e adicione-as automaticamente à
definição da tabela"*. Registros antigos recebem `NULL` na coluna nova —
leituras anteriores continuam funcionando sem mudança.

**Por que não bastou só isso**: mesmo com `mergeSchema=true`, um segundo erro
aparecia.

### Passo 2 — Reordenar as colunas do DataFrame explicitamente

```python
known_columns = [
    "event_id", "customer_id", "event_type", "occurred_at", "updated_at",
    "channel", "properties", "_ingested_at", "_source_file", "_batch_id",
]
drift_columns = sorted(c for c in df.columns if c not in known_columns)
df = df.select(*known_columns, *drift_columns)
```

**Por que esse passo foi necessário**: o `spark.read.json()` retorna colunas
em ordem alfabética — não na ordem em que a tabela as declara. O Iceberg exige
que as colunas conhecidas venham na ordem do schema da tabela, com as colunas
novas (drift) no final. Sem a reordenação:

```
IllegalArgumentException: Column is out of order
```

**Por que `drift_columns` ficam no final**: por design do Iceberg — colunas
adicionadas por schema evolution ficam após as colunas originais. Isso garante
que leituras antigas que não esperam a coluna nova continuam funcionando sem
alteração.

### Por que dois passos e não um — resumo para o code review

*"O `mergeSchema=true` resolve o problema de schema — o Iceberg aceita a coluna
nova. Mas o Spark lê JSON em ordem alfabética, e o Iceberg exige que as colunas
conhecidas venham na ordem em que a tabela as declara. Os dois problemas são
independentes e precisam de soluções independentes."*

---

## Por que `properties` como STRING é a decisão correta para a Bronze

A Bronze tem um princípio central: **fiel à raw, sem transformação de negócio**.
Manter `properties` como STRING respeita esse princípio por três razões:

1. **Preserva o payload original**: qualquer conteúdo é válido como string —
   sem perda de campos, sem dependência das chaves específicas de cada
   `event_type`.

2. **Isola o parsing**: a Silver e a Gold decidem o que extrair de `properties`
   com `get_json_object()`, de forma controlada e documentada. A Bronze não
   toma essa decisão.

3. **Torna o drift interno irrelevante na Bronze**: uma chave nova dentro de
   `properties` (ex: `properties.resolution` aparece em `ticket_replied` no
   batch 2) não afeta nada na Bronze — é só uma string diferente.

Essa decisão conecta com o que foi feito na Gold:
```sql
-- A Gold extrai só o que precisa, quando precisa
get_json_object(properties, '$.ticket_id') AS ticket_id
```

---

## Por que `bronze_customers.py` NÃO recebeu o tratamento de schema drift

Um erro comum seria aplicar `mergeSchema=true` também no `bronze_customers.py`
"por simetria". Isso causou um bug real durante a implementação:

```
IllegalArgumentException: Cannot change column type: is_active: string -> boolean
```

**Causa**: `is_active` chega do Postgres como booleano nativo (`true`/`false`).
A tabela Bronze declara `is_active` como `STRING` por design — a conversão para
`BOOLEAN` é trabalho da Silver. Sem `mergeSchema`, o Spark faz um cast implícito
de boolean para string no append (comportamento padrão, correto). Com
`mergeSchema=true`, o Spark interpreta a diferença de tipo como uma tentativa
de **evoluir o tipo da coluna** — e o Iceberg recusa essa mudança porque
`STRING → BOOLEAN` não é uma evolução válida (seria uma mudança incompatível).

**Lição**: aplicar a mesma correção em dois arquivos sem confirmar que o
problema existe nos dois é arriscado. O fix do `events` e o do `customers` são
conceitualmente diferentes porque os dois jobs têm situações diferentes:
- `events`: ganha coluna nova (drift) → `mergeSchema=true`.
- `customers`: só precisa de cast de tipo + reordenação → sem `mergeSchema`.

---

## Alternativas de tratamento — com trade-offs

### Alternativa 1 — Schema-on-read (sem `CREATE TABLE` explícito)

```python
# Iceberg infere o schema do primeiro DataFrame escrito
df.writeTo("bronze.events").createOrReplace()
```

**Vantagem**: zero configuração, absorve qualquer drift automaticamente.

**Trade-offs**:
- Sem schema declarado, campos críticos como `event_id` podem sumir sem aviso.
- Consultas que referenciam colunas pelo nome podem quebrar se o nome mudar.
- Perde a documentação implícita que o `CREATE TABLE` fornece.
- **Não adequado para produção** onde confiabilidade do schema importa.

---

### Alternativa 2 — Rejeitar o drift e alertar (pipeline para)

```python
tabela_cols = set(spark.table("bronze.events").columns)
df_cols = set(df.columns)
drift = df_cols - tabela_cols - {"_ingested_at", "_source_file", "_batch_id"}

if drift:
    raise RuntimeError(f"Schema drift detectado: {drift}. Revisar antes de continuar.")
```

**Vantagem**: nenhum dado inesperado entra sem aprovação humana. Postura mais
conservadora e segura para sistemas críticos.

**Trade-offs**:
- O pipeline para completamente até um engenheiro analisar e aprovar.
- Em APIs que evoluem frequentemente, gera alertas constantes e fadiga.
- **Adequado quando**: o schema é um contrato formal entre times, e mudanças
  não comunicadas são um problema sério de governança.

---

### Alternativa 3 — Quarentena dos registros com drift

```python
# Registros sem drift → tabela normal
df_normal = df.select(*known_columns)
df_normal.writeTo("bronze.events").append()

# Registros com drift → tabela de quarentena para análise
drift_cols = [c for c in df.columns if c not in known_columns]
if drift_cols:
    df_drift = df.filter(F.col(drift_cols[0]).isNotNull())
    df_drift.writeTo("bronze.events_quarantine").createOrReplace()
```

**Vantagem**: a tabela principal permanece estável; registros com schema novo
ficam disponíveis para análise sem bloquear o pipeline.

**Trade-offs**:
- Complexidade de gerenciar duas tabelas.
- Registros ficam "partidos" — joins posteriores ficam mais complexos.
- A quarentena precisa ser monitorada e resolvida periodicamente.
- **Adequado quando**: o volume de registros afetados é pequeno e você quer
  analisá-los separadamente antes de incorporar ao fluxo principal.

---

### Alternativa 4 — Data contracts com validação prévia

Usar uma ferramenta como Great Expectations ou Soda para definir o schema
esperado como um contrato explícito e versionado:

```python
# great_expectations
expect_column_to_exist("event_id")
expect_column_values_to_not_be_null("event_id")
# Se um campo novo aparecer fora do contrato → alerta/bloqueio configurável
```

**Vantagem**: detecta drift antes de chegar na tabela Iceberg, com mensagens
de erro muito mais claras. Permite configurar granularmente o que bloqueia e
o que apenas alerta.

**Trade-offs**:
- Dependência de ferramenta externa.
- Overhead de manutenção dos contratos (atualizar quando o schema mudar
  intencionalmente).
- **Adequado para**: pipelines em produção com múltiplos times consumindo
  os mesmos dados — o contrato é a interface formal entre produtor e consumidor.

---

## Resumo das decisões tomadas no projeto

| Decisão | Justificativa |
|---|---|
| `properties` → STRING na Bronze | Campo semiestruturado com schema variável por `event_type`; preservar o payload original é responsabilidade da Bronze |
| `source_app` → `mergeSchema=true` + reordenação | Drift confirmado no batch 2; absorver automaticamente é correto para Bronze que não tem schema rígido de negócio |
| NÃO aplicar `mergeSchema` em `customers` | `customers` não tem drift; aplicar causaria conflito de tipo em `is_active` (boolean vs string) |
| Cast explícito de `is_active` para STRING | Bronze declara `is_active` como STRING por design; a conversão para BOOLEAN é responsabilidade da Silver |

---

## Perguntas prováveis do code review

**"Por que dois passos para tratar o `source_app`?"**
→ `mergeSchema=true` resolve o schema — o Iceberg aceita a coluna nova. A
reordenação resolve um problema independente de ordering — o Iceberg exige que
colunas conhecidas venham antes das novas. São dois problemas, duas soluções.

**"Por que não aplicou o mesmo tratamento de drift em `customers`?"**
→ `customers` não tem schema drift no batch 2. Aplicar `mergeSchema` causou
um bug real: o Iceberg interpretou a diferença de tipo em `is_active`
(boolean vs string) como tentativa de evolução de tipo — que é uma operação
inválida. Aprendi que a mesma solução não é necessariamente simétrica.

**"Por que `properties` ficou como STRING e não como struct?"**
→ O schema interno do `properties` varia por `event_type` e pode mudar a
qualquer batch. Como STRING, qualquer conteúdo é válido — a Bronze preserva
o payload e a Silver/Gold extraem o que precisam com `get_json_object()`.

**"Como você detectaria drift em produção antes de quebrar o job?"**
→ Um check de qualidade que compara as colunas do DataFrame recebido com o
schema atual da tabela antes de escrever — alertando ou bloqueando conforme
a severidade configurada. Ou data contracts via Great Expectations.