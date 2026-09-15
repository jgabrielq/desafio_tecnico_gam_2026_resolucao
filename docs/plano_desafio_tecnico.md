# Plano de Execução — Desafio Técnico Lakehouse On-Premises

Guia estratégico para o desafio (MinIO · Iceberg · Spark · Trino · Airflow).
Prazo: entrega quarta-feira 16/09 às 16h. Implementação: segunda 14/09 + terça 15/09. Quarta: revisão e teoria.

---

## Cronograma sugerido

| Bloco | Quando | Entregas |
|---|---|---|
| **1. Arquitetura e decisões** | Seg manhã/tarde | Modelagem completa, schemas, estratégia de particionamento, desenho do SCD2, esqueleto do ARCHITECTURE.md |
| **2. Ingestão (Parte 1)** | Seg tarde/noite | Script de ingestão API + Postgres → raw zone, com retry/backoff, watermark, idempotência |
| **3. Bronze + Silver (Parte 2)** | Ter manhã | Jobs PySpark, tabelas Iceberg, MERGE INTO, dedup, SCD2 |
| **4. Gold + Trino (Parte 3)** | Ter tarde | Tabelas gold, 3 queries analíticas, resultados em CSV + interpretação |
| **5. Qualidade (Parte 4)** | Ter tarde/noite | 5+ checks com severidade, persistidos em tabela Iceberg |
| **6. DAG + Docs (Parte 5)** | Ter noite | DAG do Airflow, README, ARCHITECTURE.md finalizado |
| **7. Revisão** | Qua manhã | Teste da sequência de idempotência, ajustes, preparação para o code review |

**Regra de ouro do enunciado**: *"Entrega incompleta bem documentada vale mais do que entrega completa sem explicação."* Se o tempo apertar, priorize documentar o que faltou e como faria.

---

## Perguntas que valeria fazer ao time

O enunciado diz explicitamente: *"qualquer dúvida sobre o enunciado, pergunte. Saber perguntar faz parte da avaliação."* Estas são ambiguidades genuínas — perguntar demonstra maturidade, mas **não espere a resposta para começar**: decida, documente, e ajuste se necessário.

1. **Modelagem histórica do cliente**: "Para responder *qual era o plano do cliente na data do evento*, vocês esperam SCD Tipo 2 completo (com `valid_from`/`valid_to`) na `silver.customers`, ou uma abordagem mais simples como snapshot por batch é suficiente?"
   → *Decisão a tomar sem esperar*: implementar SCD Tipo 2. É a resposta mais completa e demonstra domínio de modelagem dimensional.

2. **Watermark storage**: o enunciado diz *"onde você guarda esse estado é decisão sua — justifique"*. Não precisa perguntar, mas vale mencionar no README que considerou as alternativas (tabela Iceberg de controle, arquivo no MinIO, Airflow Variable, tabela no Postgres) e por que escolheu a sua.

3. **Escopo do "últimos 30 dias da base"** (query 1 da Gold): "Os últimos 30 dias devem ser calculados a partir da data máxima presente nos dados, ou da data corrente de execução?"
   → *Decisão sem esperar*: usar `MAX(occurred_at)` da base, já que o dataset é sintético e fixo. Documentar essa escolha.

4. **Tratamento de `customer_id` órfão**: "Eventos com `customer_id` inexistente na dimensão devem ser descartados na silver, mantidos com flag, ou direcionados para uma tabela de quarentena?"
   → *Decisão sem esperar*: manter na silver com flag (`_customer_exists = false`) + registrar no log de qualidade. Nunca descartar dado silenciosamente.

5. **Definição de "primeira resposta"** (query 2): "Se um ticket tem múltiplos `ticket_replied`, consideramos o primeiro cronologicamente por `occurred_at`? E se houver replies antes do `ticket_opened` por problema de ordenação?"

---

## Parte 1 — Ingestão (raw zone)

### Decisões de arquitetura

**Formato da raw**: JSON comprimido (`.json.gz`), conforme sugerido no enunciado. Preserva o payload exatamente como veio, sem transformação — inclusive o `properties` semiestruturado e eventuais campos novos do schema drift.

**Particionamento**: `raw/events/ingestion_date=YYYY-MM-DD/part-NNN.json.gz` e `raw/customers/ingestion_date=YYYY-MM-DD/part-000.json.gz`.

**Idempotência da raw**: como o enunciado exige "rodar duas vezes o mesmo dia não pode corromper a raw nem duplicar arquivos", a estratégia é **overwrite da partição do dia** — apagar todos os objetos sob `ingestion_date=YYYY-MM-DD/` antes de escrever. Alternativa considerada: nomes de arquivo determinísticos (mesmo nome sobrescreve), mas overwrite de partição é mais seguro se o número de páginas variar entre execuções.

**Watermark**: recomendo uma **tabela Iceberg de controle** (`lakehouse.control.watermarks`) com colunas `source`, `last_updated_at`, `updated_at_control`. Justificativa para o README:
- Fica no mesmo lakehouse (não depende de infraestrutura externa como Airflow Variables).
- É auditável e versionada (Iceberg tem time travel — dá para ver o histórico de watermarks).
- Sobrevive a restart do Airflow.
- Alternativas descartadas: Airflow Variable (acopla o pipeline ao orquestrador), arquivo no MinIO (sem transação/ACID), tabela no Postgres (mistura o storage de controle com uma das fontes).

### Tratamento dos desafios da API

| Desafio | Estratégia |
|---|---|
| Paginação | Loop até `page == total_pages`, com `page_size` fixo |
| 429 (rate limit) | Retry com backoff exponencial + respeitar header `Retry-After` se presente. Throttle preventivo: manter <10 req/s |
| 503 (2% das chamadas) | Retry com backoff exponencial (`tenacity` ou implementação manual), máx. 5 tentativas |
| Duplicatas entre páginas | Não tratar na raw (raw preserva tudo). Dedup acontece na silver |
| Registros corrigidos | Não tratar na raw. Na silver, MERGE mantendo maior `updated_at` |
| Late arrival | Ingestão por `updated_at` (não `occurred_at`) já captura naturalmente |
| Schema drift | Ingerir JSON sem schema fixo. Bronze usa schema evolution do Iceberg |
| `customer_id` nulo/órfão | Preservar na raw. Flag + quarentena na silver |

### Bibliotecas sugeridas

- `requests` — chamadas HTTP
- `tenacity` — retry com backoff declarativo (`@retry(wait=wait_exponential(), stop=stop_after_attempt(5))`)
- `boto3` — escrita no MinIO (S3 API)
- `psycopg2` / `sqlalchemy` — extração do Postgres
- `structlog` ou `logging` com formatter JSON — logs estruturados (o enunciado pede: contagem de registros, páginas lidas, retries, duração)

### Logs estruturados exigidos

```python
logger.info("ingestao_concluida", extra={
    "source": "api_events",
    "ingestion_date": "2026-03-11",
    "records": 18432,
    "pages_read": 37,
    "retries_429": 3,
    "retries_503": 1,
    "duration_seconds": 42.7,
})
```

---

## Parte 2 — Bronze e Silver (PySpark + Iceberg)

### Bronze — append-only, fiel à raw

Colunas de controle exigidas: `_ingested_at`, `_source_file`, `_batch_id`.

**Decisão sobre schema drift**: Iceberg suporta **schema evolution** nativamente — adicionar uma coluna nova não quebra leituras antigas nem exige reescrever dados. Configurar `spark.sql.iceberg.check-ordering=false` e usar `mergeSchema` na escrita. Documentar isso no ARCHITECTURE.md como resposta direta ao requisito "seu pipeline não pode quebrar por causa disso".

**Append-only + idempotência**: aparente contradição — se é append-only, como rodar duas vezes não duplica? Estratégia: usar `_batch_id` derivado determinísticamente da partição de ingestão, e antes do append, deletar registros daquele `_batch_id` (`DELETE FROM bronze.events WHERE _batch_id = '...'`). Iceberg suporta DELETE eficiente (não reescreve o arquivo inteiro). Documentar essa escolha.

### Silver — dedup com MERGE INTO

**`silver.events`**:
- Chave de negócio: `event_id`
- Dedup: manter versão com maior `updated_at` (resolve tanto duplicatas entre páginas quanto registros corrigidos)
- `MERGE INTO ... WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`
- `properties`: manter como `MAP<STRING, STRING>` ou coluna JSON string com função de acesso. Recomendo `MAP` para permitir consulta no Trino sem parsing
- Flag `_customer_exists` para eventos órfãos

**`silver.customers` — SCD Tipo 2**:
```
customer_id, company_name, plan, segment, signup_date, country, is_active,
updated_at, valid_from, valid_to, is_current
```
- `valid_from` = `updated_at` do registro
- `valid_to` = `updated_at` da próxima versão, ou `'9999-12-31'` se atual
- `is_current` = boolean para facilitar filtros

Isso responde diretamente ao "qual era o plano do cliente na data do evento": join com `e.occurred_at BETWEEN c.valid_from AND c.valid_to`.

### Particionamento — justificativa exigida

**`silver.events`**: `PARTITIONED BY (days(occurred_at))`
- **Por quê**: as três queries da Gold filtram por janela temporal (últimos 30 dias, por mês, por coorte mensal). Particionar por dia permite partition pruning agressivo.
- **Por que não por `event_type`**: baixa cardinalidade (poucos tipos), geraria partições muito desbalanceadas, e as queries não filtram primariamente por tipo.
- **Por que não por `customer_id`**: altíssima cardinalidade → problema grave de small files (milhares de partições minúsculas).
- **Impacto de particionar diferente**: documentar que `months(occurred_at)` reduziria o número de arquivos (menos small files) mas perderia granularidade de pruning para a query dos últimos 30 dias.

**`silver.customers`**: sem particionamento (tabela pequena, dimensão). Justificar: particionar uma dimensão de poucos milhares de linhas cria overhead sem benefício.

---

## Parte 3 — Gold + Trino

### Estratégia

Criar tabelas gold materializadas (não views) para as três perguntas, porque:
- O enunciado pede "tabelas gold que julgar necessárias"
- Materializar demonstra entendimento da camada gold como produto de dados pré-calculado
- Permite adicionar colunas de controle (`_generated_at`)

### As três queries

**1. Top 10 clientes por volume (últimos 30 dias)**
- Join `silver.events` × `silver.customers` (versão vigente na data do evento, via SCD2)
- `WHERE occurred_at >= (SELECT MAX(occurred_at) FROM silver.events) - INTERVAL '30' DAY`
- `GROUP BY customer_id, company_name, plan, segment ORDER BY COUNT(*) DESC LIMIT 10`

**2. Tempo médio até primeira resposta**
- Self-join ou window function em `silver.events` filtrando `event_type IN ('ticket_opened', 'ticket_replied')`
- Extrair `ticket_id` de `properties`
- `MIN(occurred_at)` do reply por ticket, menos o `occurred_at` do opened
- **Tickets sem resposta**: `LEFT JOIN` + reportar contagem separadamente (não excluir silenciosamente — o enunciado pede "trate explicitamente")
- Agrupar por plano (do SCD2 na data do ticket) e por mês

**3. Retenção por coorte**
- Coorte = `DATE_TRUNC('month', signup_date)`
- Para cada mês subsequente, % de clientes da coorte com ≥1 evento
- Window function ou CTE com cross join de coortes × meses

**Interpretação**: o enunciado pede "um parágrafo interpretando cada número" — não pule isso, vale pontos em documentação.

---

## Parte 4 — Qualidade e observabilidade

### Design

Tabela `lakehouse.quality.check_results`:
```
check_name, check_type, severity, status, table_name, expected, actual,
executed_at, batch_id, details
```

### Os 5+ checks exigidos

| Check | Severidade | Comportamento |
|---|---|---|
| Unicidade de PK na silver | **BLOCKING** | Para o pipeline — duplicata em chave de negócio indica falha no MERGE |
| Integridade referencial (`events.customer_id` → `customers`) | **WARNING** | Alerta — o enunciado já avisa que existem órfãos intencionais |
| Volumetria fora do esperado (vs. média dos dias anteriores) | **WARNING** | Alerta — variação pode ser legítima |
| Valores de domínio inválidos (`event_type`, `plan`) | **WARNING** | Alerta + quarentena dos registros |
| Freshness (dado mais recente dentro da janela) | **BLOCKING** | Para — dado velho indica falha na ingestão |
| *(bônus)* Taxa de nulos em colunas críticas | WARNING | Alerta |

### Como conectar a alerta real (exigido pelo enunciado)

Descrever no README: `on_failure_callback` do Airflow disparando webhook para Slack/PagerDuty, com payload contendo `check_name`, `severity`, `expected` vs `actual`, e link para o log da task. Para checks WARNING, acumular num digest diário em vez de alertar individualmente (evita fadiga de alerta).

---

## Parte 5 — DAG do Airflow

```
[sensor_fontes_disponiveis]
    ↓
[ingestao_api] ──┐
                 ├──→ [bronze_events] ──┐
[ingestao_postgres] ──→ [bronze_customers] ──┤
                                              ├──→ [quality_bronze]
                                              ↓
                             [silver_events] + [silver_customers]
                                              ↓
                                      [quality_silver]  ← BLOCKING
                                              ↓
                                        [gold_tables]
                                              ↓
                                      [quality_gold]
```

**Requisitos explícitos do enunciado**: dependências, retries, `catchup`/backfill, parâmetro de data de execução (`{{ ds }}`).

**Decisões a documentar**:
- `catchup=True` + `max_active_runs=1` (evita concorrência no lakehouse)
- `retries=2` com `retry_delay=timedelta(minutes=5)`
- Todas as tasks recebem `{{ ds }}` — nenhuma usa `datetime.now()`
- Quality checks BLOCKING como tasks que falham (não apenas logam), interrompendo o fluxo downstream

---

## Bônus — quando discutir cada um

Serão avaliados no momento certo da implementação, não antes:

| Bônus | Quando faz sentido | Esforço |
|---|---|---|
| Testes pytest das transformações | Após Silver estar funcionando | Médio — alto ROI, demonstra engenharia de software |
| Manutenção Iceberg (`rewrite_data_files`, `expire_snapshots`, time travel) | Após Silver, como task adicional na DAG | Baixo — alto impacto, responde diretamente à pergunta 5 do ARCHITECTURE.md |
| GitHub Actions (lint + testes) | Após ter testes | Baixo |
| dbt (silver→gold) | Só se sobrar tempo | Alto — reescreveria a Gold inteira |
| LLM para qualidade de dados | Só se sobrar tempo | Médio — interessante, mas arriscado como último item |

**Recomendação**: priorizar **manutenção Iceberg** (baixo esforço, responde uma pergunta obrigatória do ARCHITECTURE.md) e **testes pytest** (demonstra maturidade de engenharia). Os demais só se o obrigatório estiver 100%.

---

## ARCHITECTURE.md — as 6 respostas exigidas

Preparar respostas para:

1. **Particionamento e impacto de particionar diferente** → ver seção Parte 2.
2. **Idempotência em cada etapa** → raw (overwrite de partição), bronze (delete por `_batch_id` + append), silver (MERGE INTO por chave de negócio), gold (overwrite completo — tabelas pequenas).
3. **Modelagem da mudança de plano** → SCD Tipo 2, justificando vs. alternativas (snapshot por batch perde granularidade; SCD Tipo 1 perde histórico inteiramente).
4. **Volume 100×: o que quebraria primeiro** → provavelmente a ingestão da API (rate limit de 10 req/s vira gargalo linear — 100× mais dados = 100× mais tempo). Segundo ponto: small files na bronze append-only. Soluções: paralelizar ingestão por janela de tempo, compactação agressiva.
5. **Small files no Iceberg** → sim, existe (ingestão diária de volume pequeno gera muitos arquivos pequenos). Solução: `rewrite_data_files` periódico, `write.target-file-size-bytes` configurado, e considerar particionamento menos granular se o volume por partição for baixo.
6. **O que faria diferente com mais uma semana** → ser honesto: testes mais abrangentes, dbt para a camada gold, CDC real em vez de batch, observabilidade com métricas (não só logs), data contracts entre camadas.

---

## Regras do desafio — atenção especial

**Uso de IA**: o enunciado **permite e incentiva**, mas exige duas coisas:
1. **Declarar no README onde usou** — seja específico e honesto (ex: "usei Claude para estruturar a lógica de retry e revisar o particionamento; toda a modelagem e decisões de arquitetura foram minhas").
2. **Estar preparado para explicar linha por linha** — por isso a implementação deve ser feita no formato que viemos usando: entender cada bloco antes de seguir. No code review eles vão **pedir alterações ao vivo**.

**O que NÃO estão avaliando**: interface gráfica, volume de dados, conhecimento prévio de Iceberg/Trino, quantidade de linhas de código. Não gaste tempo nisso.

**Peso do code review**: 60 minutos, pesa tanto quanto o código. Preparar-se para defender cada decisão é tão importante quanto implementar.
