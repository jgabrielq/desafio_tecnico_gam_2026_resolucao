## PARTE 1: Ingestão na camada Bronze via Python puro

### 1.1. Funcionamento do ``config.py``

Por que usamos o ``config.py`` em vez de ler o ``.env`` diretamente em cada arquivo?

O ``config.py`` atua como uma ponte (Single Source of Truth / Fonte Única da Verdade) entre o arquivo ``.env`` e o restante do código em Python. As principais razões para usar o ``config.py`` são:

- Princípio DRY (Don't Repeat Yourself): Sem o ``config.py``, cada arquivo (``api_client.py, storage.py, watermark.py``) precisaria chamar ``load_dotenv()``, fazer ``os.getenv()``, converter tipos e tratar fallbacks repetidamente. Com o ``config.py``, fazemos isso uma única vez e todos os outros módulos apenas fazem ``from ingestion import config``
- Valores Padrão (Fallbacks): Se por algum motivo uma variável não estiver definida no ``.env`` do ambiente de execução, o ``config.py`` define um valor padrão seguro para desenvolvimento local (ex: "http://localhost:9000").

---

### 1.2. Funcionamento do 'watermark.py'

Controlar o estado da ingestão incremental, lendo e gravando o arquivo de controle ``raw/control/watermarks.json`` no bucket do MinIO. Se a camada de ingestão usasse o Spark para o processamento dos dados - como o caso de volume ser muito grande - poderíamos armazenar o ``watermark`` em uma tabela do Iceberg.

Nesse desafio, os registros têm um campo `updated_at` que representa "quando esse registro foi criado ou atualizado pela última vez". O `watermark` aqui é o maior `updated_at` que você já processou com sucesso — é um ponteiro que diz "da próxima vez, me traga só o que mudou depois desse momento". De forma geral, nessa estrutura de controle, nós registramos dois campos de controle para cada fonte processada. O campo ``last_updated_at`` corresponde a data dos registros mais recente de cada fonte que foram processados. Já o campo ``updated_at`` corresponde a data mais recente do processamento que foi realizado para aquele ``last_updated_at``. Ou seja, é um metadado da própria pipeline.

- ``last_updated_at``: Representa a data do dado (maior timestamp de modificação encontrado nos registros extraídos daquela fonte). É esse valor que o pipeline consulta e utiliza como filtro since na execução seguinte para trazer apenas os incrementos novos
- ``updated_at``: Representa a data da execução do pipeline (timestamp de quando o arquivo de watermark foi gravado no MinIO). Serve estritamente como metadado de auditoria para saber quando aquela marcação foi atualizada pela última vez.

**O papel do `watermark` na ingestão incremental (Exemplo)**

**Execução 1:**
  → API retorna eventos com `updated_at` entre `2026-01-01` e `2026-03-11`
  → Você processa tudo
  → Salva `watermark`: "2026-03-11T14:02:11Z" (o maior `updated_at` visto)

**Execução 2 (no dia seguinte):**
  → Você lê o watermark: "2026-03-11T14:02:11Z"
  → Chama a API com ?since=2026-03-11T14:02:11Z
  → API retorna só o que mudou depois desse ponto
  → Processa só os novos/modificados
  → Atualiza o watermark para o novo máximo

Conectando com o late arrival

Aqui entra uma nuance importante que o desafio menciona: late arrival de até 3 dias. Isso significa que um evento com ``occurred_at`` de 3 dias atrás pode aparecer na API hoje com ``updated_at`` = hoje. Se você filtrar por ``occurred_at`` >= ``watermark``, perderia esse evento. Mas se filtrar por ``updated_at`` >= ``watermark``, captura corretamente — porque o ``updated_at`` reflete quando o dado ficou disponível na API, não quando o evento aconteceu de fato.

*Conclusão*: o ``watermark`` deve ser baseado em ``updated_at``, não em ``occurred_at``. Isso é uma decisão de arquitetura que você vai documentar no ARCHITECTURE.md.

#### FUNÇÃO: `read_watermark()`:

A função tenta ler e decodificar o arquivo JSON do MinIO. Se conseguir, busca e retorna a string ``last_updated_at`` da fonte solicitada. Se o arquivo não existir (NoSuchKey) ou o JSON estiver corrompido, ela entende que é a primeira carga daquela fonte e retorna None. Se ocorrer qualquer outro erro com o MinIO (como queda de rede, timeout ou erro de credencial), ela lança a exceção (raise) e interrompe o pipeline para evitar processamento inconsistente. 

#### FUNÇÃO: `write_watermark()`:

Esta função salva a informação de até onde os dados foram ingeridos. 
Por que ler antes de gravar? Porque o arquivo ``watermarks.json`` armazena o watermark de todas as fontes (API e Postgres). Se simplesmente gravássemos o watermark da API, apagaríamos o estado do Postgres. Portanto, primeiro baixamos o JSON existente (se houver) e atualizamos apenas a seção correspondente à fonte atual.

**DÚVIDAS**:

1. Por que não usar uma tabela Iceberg para persistir o `watermark` e ter os benefícios de tornar os dados auditáveis, versionados e com ACID?

Escolhi persistir o `watermark` como um arquivo JSON no MinIO (raw/control/watermarks.json) porque a etapa de ingestão é implementada em Python puro, sem Spark. Ler o `watermark` de uma tabela Iceberg exigiria instanciar uma SparkSession no script de ingestão — adicionando uma dependência pesada a um processo que precisa ser leve e rápido. Como o script já usa boto3 para gravar na raw zone, ler e escrever um arquivo JSON de controle no mesmo MinIO não adiciona nenhuma dependência nova. Para o volume e criticidade desse pipeline, o arquivo JSON é suficiente, simples de auditar, e coerente com a separação de responsabilidades: Python cuida da ingestão e do controle de estado; Spark cuida das transformações.

---

### 1.3. Funcionamento do 'api_client.py'

É responsável por extrair os dados da API com tratamento quanto a Paginação, throttle, retry 429/503 e backoff exponencial.

#### FUNÇÃO: `_fetch_page_with_retry()`: a função privada de retry

O underscore no nome (_fetch_page_with_retry) é uma convenção Python que sinaliza "função interna, não use fora deste módulo". Separar o retry numa função própria é boa prática: mantém fetch_all_events focada na lógica de paginação, e o retry isolado e testável independentemente. Possui o loop de retry com contador explícito — mais claro que for i in range(max_attempts) porque permite continue sem perder o controle do número de tentativas.

- Tratamento do 429: O Retry-After é um header HTTP padrão que o servidor envia dizendo "espere X segundos antes de tentar de novo". Respeitar esse header é o comportamento correto e educado — você não está "adivinhando" quanto tempo esperar, está obedecendo o que a API pediu. O fallback de 60s é conservador mas seguro caso o header não venha.
- Tratamento do 5xx: ``2 ** (attempt - 1)`` é o backoff exponencial — a cada falha, o tempo de espera dobra. Isso é fundamental para não sobrecarregar um servidor já em dificuldades: se ele está falhando com 503, continuar bombardeando requisições só piora a situação. O backoff dá tempo para o servidor se recuperar. Na última tentativa (attempt == max_attempts), raise_for_status() lança uma exceção com o status HTTP, interrompendo a execução.
- Tratamento de 4xx:: Chega aqui quando o status não é 429, não é 5xx — ou seja, qualquer 4xx (exceto 429 que já foi tratado). Erros 4xx são erros do cliente (você mandou algo errado — chave inválida, parâmetro malformado), então não faz sentido retentar: o resultado será o mesmo. raise_for_status() lança imediatamente.
- Tratamento de erros de rede: Erros de conexão e timeout são diferentes de erros HTTP — são problemas de rede, não de resposta do servidor. O comportamento é o mesmo do 5xx (retry com backoff), e faz sentido agrupa-los no mesmo contador retries_503 porque a causa raiz é similar: o servidor está inacessível. Se esgotou todas as tentativas sem sucesso, a exceção final é clara e acionável — diz exatamente qual página falhou e quantas tentativas foram feitas. Isso facilita muito o debugging quando o log de erro aparece.

#### FUNÇÃO: `fetch_all_events()`: a função pública

Recebe since (o watermark lido antes) e retorna uma tupla com dois elementos — a lista de registros e um dicionário de métricas. Retornar as métricas junto com os dados é uma boa prática: quem chama essa função recebe tudo que precisa para logar e monitorar, sem precisar de variáveis globais.
O fallback para 1970-01-01 (epoch Unix) é a primeira execução — busca tudo desde o início da história. É um valor seguro porque qualquer updated_at real será posterior a ele.
Inicializar total_pages = 1 é um padrão inteligente: o loop while page <= total_pages vai rodar pelo menos uma vez (buscando a página 1), e só depois atualizar total_pages com o valor real que veio no response. Evita a necessidade de uma chamada prévia só para descobrir quantas páginas existem.
O time.sleep(0.1) é o throttle preventivo — garante no máximo 10 requisições por segundo (1 req / 0.1s = 10 req/s), exatamente no limite da API. Sem isso, o loop poderia disparar 50+ requisições por segundo e receber uma enxurrada de 429s.

**DÚVIDAS**:

**1. A escolha do ``while`` vs ``for i in range(max_attempts)``**

A diferença não é de performance — é de clareza de intenção e flexibilidade de controle:
- Com for i in range(max_attempts), você itera sobre uma sequência fixa de índices. O problema: quando você usa continue dentro de um for, o índice avança automaticamente — você perde uma tentativa "de graça" sem ter feito nada útil com ela. Para contornar isso, teria que usar lógica extra para não contar o continue como tentativa. Com while, você controla explicitamente quando o contador avança (attempt += 1 no início do loop) — o continue volta para o topo do while sem incrementar nada extra, e a tentativa só "conta" quando você efetivamente fez a requisição.

Existe ainda um motivo mais sutil: o for i in range() comunica "vou iterar sobre uma coleção de valores". O while attempt < max_attempts comunica "vou continuar tentando até uma condição ser satisfeita" — que é exatamente o que retry significa semanticamente. O código fica mais legível porque a estrutura de controle reflete a intenção real.

**2. list[dict] e memória — você deve sim se preocupar em contextos maiores**

No contexto atual (dataset pequeno por design, como o enunciado afirma), não é um problema. Mas a pergunta é legítima e tem uma resposta arquitetural importante.

- O problema da lista de dicts em memória: você carrega todas as páginas antes de escrever qualquer coisa no MinIO. Se o dataset tivesse 10 milhões de registros em 20.000 páginas, você estaria acumulando tudo na RAM antes de descarregar. Em Python, um dict por evento pode facilmente ocupar 1-2KB — 10 milhões de eventos = 10-20GB em memória.
- A solução para volumes maiores seria streaming por página: em vez de all_records.extend(page_records), você gravaria cada página (ou lote de N páginas) no MinIO assim que chegasse, e descartaria da memória. Isso muda a arquitetura de "acumula tudo, depois grava" para "processa e grava incrementalmente". O trade-off é que você precisaria de uma estratégia diferente para o arquivo de saída — ou múltiplos arquivos part-000, part-001, etc., ou um arquivo único construído com append de bytes gzip (mais complexo).

Para o desafio atual, a abordagem atual é a certa — simples, clara, e o volume não justifica a complexidade do streaming. Mas mencionar essa limitação no ARCHITECTURE.md (na pergunta "o que quebraria primeiro com 100x mais volume?") seria uma resposta muito forte.

**3. Forma alternativa — arquitetura com fila de mensagens**

A alternativa arquitetural mais comum em produção para esse tipo de ingestão é substituir o script Python síncrono por um padrão producer/consumer com fila de mensagens (ex: Kafka, RabbitMQ, AWS SQS).
Como funcionaria: em vez de um script que pagina a API sequencialmente, você teria:
- Um producer leve que só descobre as páginas disponíveis (chama a API uma vez para saber o total_pages) e publica uma mensagem por página numa fila.
Múltiplos consumers paralelos que consomem essas mensagens, cada um buscando uma página diferente da API simultaneamente, e gravando no MinIO.
- O ponto mais importante do trade-off: a fila não resolve o rate limit de 10 req/s — você continuaria precisando de throttle, só que distribuído entre os consumers. Com 3 consumers cada um respeitando 3 req/s, você chegaria nos mesmos 10 req/s totais. A vantagem real da fila aparece na resiliência: se um consumer morrer no meio de uma página, a mensagem volta para a fila e outro consumer pega — sem reprocessar tudo do início.

Para o desafio, mencionar essa arquitetura alternativa no ARCHITECTURE.md (como "o que faria diferente com mais uma semana") seria uma resposta forte sem precisar implementar.

---

### 1.4. Funcionamento do 'api_client.py'

Principais pontos deste módulo:
- RealDictCursor: Faz com que o cursor do psycopg2 retorne os dados diretamente como dicionários Python ({"customer_id": "c_01", ...}), simplificando a serialização.
- Consulta Parametrizada (%(since)s): Impede SQL Injection e deixa a execução segura e performática no Postgres[cite: 5].
- Tratamento Finito de Conexões: Uso do bloco try/finally garantindo que a conexão com o banco seja fechada mesmo em caso de erro no meio da consulta[cite: 5].

**Análise passo a passo das decisões técnicas no ``postgres_client.py``:**

**1. Cursor com RealDictCursor**

- Decisão: Usar RealDictCursor para mapear cada linha diretamente como um dicionário {coluna: valor}.
- Trade-off: Cursors padrões retornam tuplas indexadas por posição (row[0], row[1]). O cursor de dicionário aloca uma fração mínima a mais de memória por linha, mas elimina completamente o risco de erros por ordenação de colunas no código Python e prepara o dado nativamente para a escrita em formato NDJSON/S3.

**2. Conversão e Normalização de Fuso Horário no SQL (AT TIME ZONE 'UTC')**

- Decisão: Delegar ao próprio PostgreSQL a conversão do campo updated_at para UTC e do signup_date para string pura.
- Trade-off: Deixar a conversão para o Python exigiria iterar linha a linha aplicando bibliotecas como pytz ou zoneinfo, aumentando o uso de CPU da aplicação. Fazer no banco aproveita o motor otimizado do Postgres em C, garantindo que o timestamp chegue padronizado sem depender do fuso configurado no servidor da aplicação.

**3. Consulta Parametrizada (%(since)s)**

- Decisão: Passar o watermark since usando a sintaxe de parâmetros do psycopg2.
- Trade-off: Usar interpolação de strings direta (f"WHERE updated_at > '{since}'") expõe o pipeline a ataques de SQL Injection e impede o banco de dados de reaproveitar planos de execução (query plans). A consulta parametrizada envia os dados separadamente da estrutura da query, garantindo segurança e otimização.

**4. Ordenação Incremental (ORDER BY updated_at ASC)**

- Decisão: Garantir que a extração traga os dados ordenados cronologicamente.
- Trade-off: Adiciona uma etapa de ordenação no banco (que usa índices existentes). No entanto, o ganho de resiliência é enorme: se a busca de dados precisar ser interrompida por timeout, o maior updated_at extraído até aquele ponto pode ser salvo com precisão como o novo watermark, mantendo a idempotência da próxima carga.

**5. Gerenciamento de Conexão com try/finally e Fechamento Explícito**

- Decisão: Encerrar a conexão conn.close() dentro do bloco finally.
- Trade-off: Confiar no Garbage Collector do Python para encerrar conexões pode deixar sockets abertos ("conexões órfãs") no servidor Postgres. Em ambientes de orquestração como o Airflow, acúmulos de conexões abertas esgotam o pool do banco (max_connections), derrubando outros serviços dependentes.

**DÚVIDAS**:

**1. Por que não usar o SQLAlchemy + Pandas para fazer a conexão com o Postgres e extrair os dados? Tem alguma vantagem usar diretamente o 'psycopg2'?**

Usar psycopg2 diretamente com RealDictCursor é a melhor escolha para a ingestão na Raw Zone por ser mais leve e evitar transformações automáticas de dados que o Pandas realiza por padrão.  
- Preservação fiel dos dados (Fidelidade da Raw Zone): O Pandas tenta inferir e converter tipos automaticamente ao criar DataFrames (transforma None em NaN, altera tipos numéricos com nulos e manipula objetos de data). Na camada Raw, a premissa é preservar o dado exatamente como veio do Postgres.  
- Menor overhead de memória e CPU: Instanciar um DataFrame inteiro em memória para depois convertê-lo em dicionários/JSON cria alocação dupla de RAM e processamento desnecessário. O psycopg2 entrega as linhas como dicionários nativos do Python.
- Serialização limpa em NDJSON: Dicionários Python convertem direto via json.dumps(). O Pandas gera inconsistências ao exportar NaN para JSON (produzindo NaN não estandardizado em vez de null).
- Dependências enxutas: Mantém o ambiente da ingestão enxuto e rápido, sem precisar importar bibliotecas pesadas como pandas e sqlalchemy apenas para rodar um SELECT simples

**SQLAlchemy Engine vs. psycopg2 Cursor**

- Explicação Leiga: Pense no Engine como a central telefônica do banco: ele cuida das credenciais de acesso, mantém o telefone fora do gancho e sabe qual idioma o banco fala. O Cursor é o garçom que pega o seu pedido (query SQL), leva até a cozinha (banco de dados) e traz os pratos (linhas da tabela) até a sua mesa.

- Explicação Técnica: O Engine é a abstração central do SQLAlchemy que gerencia o Connection Pool (reuso de conexões) e o Dialect específico do banco. Ele não executa queries diretamente, mas fornece conexões ativas. O Cursor é um objeto de baixo nível da especificação DBAPI 2.0 implementado pelo psycopg2. Ele mantém o contexto de execução de uma instrução SQL e controla o ponteiro de memória no servidor/cliente para iterar sobre os resultados (fetchone, fetchmany, fetchall).

**2. Centralização da Formatação de Datas em um módulo ``utils.py``**

Criar uma função utilitária em um módulo dedicado (ex: ingestion/utils.py) é uma excelente ideia e segue o princípio DRY (Don't Repeat Yourself).

Isso evita ter a string "%Y-%m-%dT%H:%M:%SZ" espalhada em múltiplos arquivos. Se amanhã o requisito do projeto mudar para incluir precisão em milissegundos, você altera a regra em um único lugar sem risco de quebrar outros conectores.

**3. Listas em Memória vs. Escalabilidade**

Para volumes pequenos e médios (como os 15 mil registros da API), armazenar os registros em uma list Python é perfeitamente aceitável e rápido. Porém, para volumes massivos (milhões de linhas), essa abordagem não escala porque causa alto consumo de RAM e acarreta erros de Out-Of-Memory (OOM).

Alternativas recomendadas para grande escala:

- Geradores Python (yield): Em vez de retornar uma lista completa, a função expõe um gerador que entrega um registro por vez à medida que é consumido, mantendo o uso de memória próximo de zero.
- Processamento por Lotes (Batching / Chunking): Usar cursor.fetchmany(10000) com cursores no lado do servidor (Server-Side Cursors). O script lê 10 mil linhas, grava o arquivo parcial no MinIO, limpa a memória e busca as próximas 10 mil.
- Motores de Processamento Distribuído: Transferir a extração pesada diretamente para o PySpark, que faz a leitura paralelizada e distribuída sem passar pela memória do script Python principal.

---

### 1.5 Funcionamento do 'storage.py'

Este módulo é responsável por persistir os dados brutos na camada Raw Zone do MinIO em formato NDJSON (Newline Delimited JSON), seguindo o particionamento por ``ingestion_date`` para cada ``source``.

Por que NDJSON e Hive Partitioning (``ingestion_date``=...)?

- NDJSON (JSON Lines): Motores analíticos distribuídos (como PySpark e Trino) conseguem dividir a leitura de arquivos NDJSON em paralelo por bloco sem precisar carregar o arquivo todo em memória, diferente de um array JSON tradicional [...].
- Hive Style Partitioning: A estrutura de pastas ``raw/events/ingestion_date=YYY-MM-dd/`` permite que pipelines futuros na Bronze Zone façam partition pruning, lendo apenas lotes específicos em vez de varrer o bucket inteiro.

**DÚVIDAS**:

**1. Separação em duas funções (Serialização vs. Envio S3)**
Sim, poderíamos e é uma excelente prática de arquitetura de software (Princípio de Responsabilidade Única). Dividir o código em uma função helper _to_ndjson(records) e manter o save_raw_batch focado no envio para o S3 traz vantagens diretas:
- Facilidade para Testes Unitários: É possível testar se a conversão de lista para NDJSON trata caracteres especiais ou listas vazias sem precisar mockar o cliente S3.
- Reuso: Se outro módulo precisar transformar dicionários em NDJSON sem salvar no S3, a lógica de transformação já está isolada.

**2. De onde vem e como é formado o ``ingestion_date``?**
O ``ingestion_date`` vem do orquestrador da ingestão (o script ``ingest.py`` ou a DAG do Airflow). Ele é instanciado logo no início do processo de ingestão e repassado como parâmetro para todas as funções do ciclo. É gerado uma única vez por execução combinando a data e hora UTC da rodada, no formato YYYYMMDD_HHMMSS (ex: 20260315_060500).

**3. Vantagens do Particionamento Hive e escolha do ``ingestion_date``**
O padrão Hive Partitioning utiliza a estrutura explícita nome_da_chave=valor nos caminhos dos arquivos (ex: ``raw/events/ingestion_date=2026-03-15/data.ndjson``). Vantagens sobre outros tipos de estrutura de pastas:

- Reconhecimento Automático de Colunas (Partition Discovery): Motores analíticos como PySpark e Trino leem a estrutura de pastas e criam automaticamente uma coluna virtual chamada ``ingestion_date`` nas tabelas sem exigir parsing do caminho por expressão regular.
- Otimização de Consultas (Partition Pruning): Queries que filtram por WHERE ingestion_date = '20260315_060500' ignoram 99% dos arquivos do bucket, lendo apenas o diretório exato.
Por que particionar por ``ingestion_date`` na Raw Zone (em vez de data simples)?
- Isolamento de Execução e Idempotência: Se o pipeline rodar 5 vezes no mesmo dia (ex: a cada 4 horas), o - particionamento por data simples (dt=2026-03-15/) misturaria todas as execuções. Com o ``ingestion_date``, cada execução gera um lote isolado e rastreável.
- Facilidade de Rollback e Reprocessamento: Se uma execução falhar no meio ou ingerir dados corrompidos, basta excluir a pasta do ``ingestion_date`` correspondente sem afetar os outros lotes ingeridos no mesmo dia.

---

### 1.6 Funcionamento do 'ingest.py'

Esse script une o ciclo completo: lê os watermarks atuais no MinIO, extrai os incrementos das duas fontes, persiste os arquivos NDJSON na Raw Zone e atualiza os novos ponteiros de watermark.

**Pontos-chave do Orquestrador**
- Ciclo End-to-End: Encapsula as 4 etapas fundamentais (Ler Watermark $\rightarrow$ Extrair Fonte $\rightarrow$ Escrever NDJSON no MinIO $\rightarrow$ Atualizar Watermark).
- Controle de Watermark Dinâmico: O novo watermark só é persistido se a lista de registros não for vazia e se a gravação do NDJSON ocorrer sem exceções, prevenindo perda de dados por falhas parciais.
- Isolamento de Lote: Toda a execução compartilha exatamente o mesmo ``ingestion_date``, garantindo rastreabilidade cruzada entre os dados de events e customers gerados na mesma rodada.