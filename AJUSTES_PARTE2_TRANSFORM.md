# Ajustes — Implementação e Validação da Parte 2 (Bronze + Silver)

Este documento registra os ajustes feitos durante a implementação das camadas
Bronze e Silver (`SPEC_PART2_a_TRANSFORM.md` e `SPEC_PART2_b_TRANSFOMR.md`) que
**não estavam previstos literalmente nas specs** — foram descobertos ao rodar
os jobs de verdade contra o ambiente, e cada um tem uma motivação concreta por
trás. A ideia é documentar o "porquê", não só o "o quê", para quem for revisar
o código ou retomar o trabalho depois.

Todos os ajustes abaixo foram validados executando a sequência de teste de
ponta a ponta exigida pelo desafio:

```
pipeline (batch 1) → pipeline (batch 1 de novo) → make batch2 → pipeline → pipeline de novo
```

rodada duas vezes: uma vez que revelou os problemas, e outra — após corrigir o
código e resetar o ambiente do zero (`make clean && make up`) — que confirmou
que tudo funciona sem duplicar, perder ou quebrar dado.

---

## 1. `transform/config.py` — remoção da dependência de `python-dotenv`

**O que mudou**: removida a chamada `from dotenv import load_dotenv` /
`load_dotenv()`; o módulo passou a usar só `os.getenv(..., default)`.

**Motivação**: os jobs de `transform/` rodam **dentro do container Spark**
(`dl-spark`), para onde copiamos apenas a pasta `transform/` via `docker cp`.
O arquivo `.env` do host nunca chega lá — então `load_dotenv()` não tinha
nada para carregar, e só existia para introduzir uma dependência
(`python-dotenv`) que a imagem do Spark não tem instalada, causando
`ModuleNotFoundError` ao rodar qualquer job.

**Por que não just instalar o pacote na imagem**: seria consertar o sintoma
(reinstalar a cada novo container) em vez da causa. Os defaults do
`os.getenv()` já cobrem corretamente a rede interna dos containers
(`http://minio:9000` etc.), então o `.env`/`dotenv` nunca teve efeito prático
nesse contexto — removê-lo é a correção mais simples e definitiva.

---

## 2. `transform/bronze_events.py` — schema drift do payload da API (campo `source_app`)

**O que aconteceu**: ao rodar a Bronze contra o **batch 2**, o job quebrou com:

```
AnalysisException: [INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS]
Cannot write to `lakehouse`.`bronze`.`events`, the reason is too many data columns
```

O batch 2 introduz um campo novo no payload de eventos (`source_app`) — um
comportamento **intencional e documentado** no código da mock API
(`mock-api/app/main.py`): *"o payload cresce a partir de certa data (schema
drift)"*. A spec da Bronze já previa isso na intenção ("não forçar schema
fixo... absorver schema drift sem quebrar"), mas o `CREATE TABLE` declara um
schema fixo e o `writeTo(...).append()` simples rejeita colunas que não
existem na tabela.

**Correção aplicada — duas partes**:

1. **Evolução de schema controlada no Iceberg**:
   ```python
   spark.sql(f"""
       ALTER TABLE {config.BRONZE_NAMESPACE}.events
       SET TBLPROPERTIES ('write.spark.accept-any-schema' = 'true')
   """)
   df.writeTo(f"{config.BRONZE_NAMESPACE}.events").option("mergeSchema", "true").append()
   ```
   Isso avisa o Iceberg que a tabela pode aceitar colunas novas vindas do
   DataFrame, adicionando-as automaticamente (registros antigos recebem
   `NULL` na coluna nova) em vez de rejeitar o write.

2. **Reordenação explícita das colunas antes do `append`**: mesmo com o
   `mergeSchema` habilitado, o Iceberg exige que a **ordem física** das
   colunas do DataFrame bata com a ordem final da tabela (colunas
   conhecidas, na ordem declarada no `CREATE TABLE`, seguidas pelas colunas
   novas de drift, no fim). A leitura via `spark.read.json(...)` não garante
   nenhuma ordem específica (o schema é inferido e as colunas costumam sair
   em ordem alfabética), então isso quebrava com um segundo erro
   (`IllegalArgumentException: ... is out of order`) mesmo depois do fix 1.
   A correção:
   ```python
   known_columns = [
       "event_id", "customer_id", "event_type", "occurred_at", "updated_at",
       "channel", "properties", "_ingested_at", "_source_file", "_batch_id",
   ]
   drift_columns = sorted(c for c in df.columns if c not in known_columns)
   df = df.select(*known_columns, *drift_columns)
   ```

**Validado**: `bronze.events` agora tem a coluna `source_app` (nula nos
registros do batch 1, populada nos do batch 2), e a contagem por `_batch_id`
ficou estável em reexecuções (idempotência preservada).

---

## 3. `transform/bronze_customers.py` — mesma reordenação de colunas + cast de `is_active`

**O que aconteceu**: aplicando por engano a mesma solução de "evolução de
schema" (`accept-any-schema` + `mergeSchema`) também em `bronze_customers.py`
(por precaução, achando que era simétrico ao `events`), o job passou a falhar
com:

```
IllegalArgumentException: Cannot change column type: is_active: string -> boolean
```

**Causa raiz**: `is_active` chega do Postgres como booleano nativo no JSON
(`true`/`false`), mas a tabela Bronze declara `is_active` como `STRING` (por
design — a spec é explícita: a conversão para `BOOLEAN` é trabalho da
Silver). Sem `mergeSchema`, o Spark faz um cast implícito de boolean para
string no append (comportamento padrão, seguro). **Com** `mergeSchema=true`,
o Spark interpreta a diferença de tipo como uma tentativa de **evoluir o
tipo da coluna** (não apenas adicionar uma coluna nova) — e o Iceberg recusa
essa mudança porque `STRING → BOOLEAN` não é uma evolução de schema válida
(perderia informação/seria uma mudança incompatível).

**Correção aplicada**:
1. **Removida** a chamada a `accept-any-schema`/`mergeSchema` deste job —
   `customers` não tem schema drift no batch 2 (só `events` ganha o campo
   novo), então essa defesa era desnecessária e ativamente prejudicial aqui.
2. **Cast explícito** de `is_active` para `string` antes do write, deixando
   o tipo já correto e sem ambiguidade para o Iceberg decidir:
   ```python
   if "is_active" in df.columns:
       df = df.withColumn("is_active", F.col("is_active").cast("string"))
   ```
3. A mesma **reordenação de colunas** do item 2 (o problema de ordem também
   se manifesta aqui, independente do schema drift).

**Lição**: aplicar a mesma correção "por simetria" em dois arquivos sem
confirmar que o problema realmente existe nos dois é arriscado — o fix do
`events` (evolução de schema) e o do `customers` (cast + reordenação) são
conceitualmente diferentes porque os dois jobs têm situações diferentes
(um ganha coluna nova, o outro só precisa de tipagem estável).

---

## 4. `ingestion/api_client.py` — filtro client-side para o `since` inclusivo da API

**O que aconteceu**: ao rodar a **pipeline completa duas vezes no mesmo dia**
(exigência explícita do critério de aceite do desafio), a segunda execução
da ingestão retornou **1 registro "novo"** mesmo sem nenhum dado novo ter
sido gerado. Como o `storage.py` grava sempre no mesmo objeto
(`raw/events/ingestion_date=<data>/part-000.json.gz`), essa segunda chamada
**sobrescreveu o arquivo raw do dia — que tinha 3660 eventos — com um
arquivo de apenas 1 registro**, uma perda de dado real.

**Causa raiz**: confirmada diretamente no código-fonte da mock API
(`mock-api/app/main.py`), que documenta esse comportamento como
intencional:

```python
"""
Comportamentos intencionais:
  * `since` é INCLUSIVO (>=), o que faz o registro de fronteira voltar a cada execução
  ...
"""
...
rows = [e for e in rows if e["updated_at"] >= since]
```

Ou seja: o watermark salvo é o `updated_at` do último registro processado, e
a próxima chamada usa esse valor como `since` — mas como o filtro da API é
`>=` (inclusivo), esse mesmo registro **sempre volta** na resposta seguinte.

**Correção aplicada** — filtro client-side estritamente exclusivo, aplicado
depois de coletar todas as páginas e antes de retornar os registros:

```python
# A API trata 'since' como inclusivo (>=), então o registro de fronteira
# (updated_at == since_param) volta a cada execução. Filtra client-side
# para manter a semântica de incremental estritamente exclusiva e evitar
# reprocessar o mesmo registro em execuções consecutivas no mesmo dia.
all_records = [r for r in all_records if r.get("updated_at", "") > since_param]
```

**Por que no cliente e não pedindo para mudar a API**: a API mock é uma
fixture do desafio — seu comportamento (`since` inclusivo) é proposital,
exatamente para testar se o pipeline de ingestão trata esse tipo de
fronteira corretamente. A responsabilidade de garantir semântica de
incremental estritamente exclusiva é do cliente, não da fonte.

**Validado**: depois do fix, uma segunda execução da ingestão no mesmo dia
(sem nenhum dado novo real) retorna corretamente **zero registros**, não
grava nenhum arquivo (a lógica `if events: ...` do `ingest.py` já cobria
esse caso) e o watermark permanece estável. O arquivo raw original não é
mais sobrescrito.

---

## Processo de validação usado

Como o segundo ajuste (item 4) só foi descoberto **depois** de já termos
gerado dado corrompido no ambiente (o arquivo raw de 3660 registros havia
sido reduzido a 1), o ambiente inteiro foi resetado do zero antes de rodar a
bateria de testes definitiva:

```bash
make clean   # derruba containers e apaga volumes (MinIO, Postgres, Iceberg)
make up      # sobe tudo de novo e valida (12/12 checks)
```

Só depois desse reset a sequência completa de teste foi executada e
confirmada sem duplicação, perda de dado ou erro:

| Etapa | Resultado |
|---|---|
| Pipeline (batch 1, run 1) | 15.535 eventos / 400 clientes na raw e na Bronze |
| Pipeline (batch 1, run 2 — idempotência) | Mesmas contagens, nenhum arquivo sobrescrito |
| `make batch2` | Fontes avançadas para o batch 2 |
| Pipeline (batch 2, run 1) | +3.659 eventos / +50 clientes; Bronze evolui schema (`source_app`); Silver aplica dedup e SCD2 |
| Pipeline (batch 2, run 2 — idempotência) | Mesmas contagens finais, nenhuma duplicação |

Resultado final na Silver: **18.658 eventos únicos**, **450 registros de
histórico de clientes** (413 correntes, 37 com mudança de plano
corretamente versionada via SCD2).
