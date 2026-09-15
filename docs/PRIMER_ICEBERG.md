### Dúvida: Analogia - Catalog vs. Namespace vs. Schema / Database**

A hierarquia no Spark + Iceberg funciona exatamente assim: ``[Iceberg Catalog]`` -> ``[Namespace (Schema)]`` -> ``[Tabela]``

- **Catalog (lakehouse):** É o ponto central de controle do lakehouse. Ele sabe onde ficam gravados os metadados no S3/MinIO e gerencia as transações ACID do repositório inteiro. Corresponde à instância do banco de dados (Database Server) em um RDBMS.  
- **Namespace (bronze, silver, gold):** É o agrupamento lógico de tabelas dentro do catálogo. Corresponde exatamente ao Schema (ou Database local) em bancos SQL convencionais (PostgreSQL, SQL Server, etc.).  
- **Table (events, customers):** A tabela física com seus arquivos Parquet e arquivos de manifesto Iceberg associados.  No código, quando chamamos ``lakehouse.bronze.events``, estamos dizendo: Navegue no catálogo ``lakehouse``, entre no ``namespace`` ``bronze`` e acesse a tabela ``events``.  

### Dúvida: O que é exatamente o Apache Iceberg?

O Apache Iceberg não é um servidor ou serviço rodando em segundo plano (como um banco Postgres ou um contêiner do MinIO), nem um cluster de processamento (como o Spark). O Iceberg é uma especificação aberta de Table Format (Formato de Tabela). Na prática, ele funciona como uma biblioteca (um conjunto de JARs/códigos) acoplada ao Spark e ao Trino. Quando a ``SparkSession`` é iniciada, essas bibliotecas são carregadas e "ensinam" o Spark a ler e escrever dados seguindo o padrão do Iceberg. 

**As 3 Camadas do Lakehouse: Onde cada peça se encaixa**

Para visualizar o Iceberg na prática, pense no Lakehouse dividido em três partes: 
- Storage (Armazenamento Bruto - MinIO/S3): Onde os bytes ficam salvos no disco.  
- Table Format (Iceberg): A camada de abstração que organiza os arquivos do MinIO para parecerem uma tabela SQL relacional com suporte a transações ACID.  
- Compute Engine (Spark/Trino): O motor de execução que roda o código Python/SQL e executa o processamento. 

**Onde ficam os arquivos ``.parquet`` e o que é uma "Tabela Iceberg"?**

As tabelas do Iceberg não são um banco relacional com um arquivo gigante único. Elas são uma coleção de arquivos físicos gravados diretamente no seu Object Storage (MinIO) no bucket lakehouse. Se você abrir o console do MinIO após rodar o ``bronze_events.py``, verá a seguinte estrutura física de pastas e arquivos no bucket:

s3a://lakehouse/bronze/events/
├── metadata/
│   ├── v1.metadata.json
│   ├── snap-839210-1-a4b.avro       (Manifest List)
│   └── a4b-m0.avro                  (Manifest File)
└── data/
    ├── batch_2026-03-11_001.parquet (Arquivos de Dados em Parquet)
    └── batch_2026-03-11_002.parquet


**Uma "Tabela Iceberg" é a união da camada de dados (os arquivos .parquet) com a camada de metadados (os arquivos ``.json`` e ``.avro``).** 

**O que são os Manifestos (Manifest Files) e a Árvore de Metadados?**

No formato antigo (Hive), uma tabela era simplesmente "uma pasta cheia de arquivos Parquet". Para rodar uma consulta, o motor precisava varrer a pasta inteira no S3 (operação lenta de LIST). 

O Iceberg resolveu isso criando uma árvore de metadados indexada baseada em arquivos de manifesto:
- **Catálogo (Iceberg Catalog):** Guarda apenas um ponteiro dizendo: "O arquivo de metadados atual da tabela ``events`` é o ``v1.metadata.json``".
- **Table Metadata (v1.metadata.json):** Armazena o schema atual da tabela, histórico de alterações de coluna e uma lista de Snapshots (versões da tabela ao longo do tempo). Cada snapshot aponta para uma Manifest List.
- **Manifest List (snap-.avro):** Arquivo contendo a lista de todos os Manifest Files que compõem aquele snapshot específico, junto com resumos de partições. 
- **Manifest File (*.avro):** Este é o índice principal do Iceberg. É um arquivo compacto em formato Avro que lista os caminhos exatos de cada arquivo ``.parquet`` de dados, acompanhado de estatísticas a nível de coluna (valores mínimo/máximo, contagem de nulos, tamanho dos arquivos).

**Por que a arquitetura de Manifestos é importante no seu código?**

Quando o ``bronze_events.py`` executa o comando ``DELETE FROM lakehouse.bronze.events WHERE _batch_id = '2026-03-11'``:
- Sem reescrever os arquivos brutos: O Spark não apaga os arquivos Parquet do MinIO imediatamente. O Iceberg apenas gera um novo Snapshot com um manifesto atualizado que "desvincula" ou marca como deletados os arquivos Parquet pertencentes àquele ``_batch_id``.  
- Isolamento e Transações ACID: Se outra pessoa estiver fazendo um SELECT via Trino na mesma tabela enquanto o job Python roda, ela continuará lendo o snapshot antigo de forma consistente (Leitura Reentrante/Snapshot Isolation). Assim que o ``.append()`` finaliza, o catálogo passa a apontar para o novo snapshot instantaneamente


**Onde exatamente fica o Iceberg? Raw Zone vs. Camada Bronze**

O Iceberg não fica diretamente acima dos arquivos .``json.gz`` brutos da Raw Zone.  A Raw Zone continua sendo composta por arquivos de texto brutos gravados diretamente pela ingestão. O Iceberg entra em ação quando o Spark lê esses JSONs, faz o ajuste de tipagem/metadados e grava os dados na Camada Bronze em formato Parquet. A partir desse momento, o Iceberg gerencia esses arquivos Parquet organizando-os com sua árvore de manifestos e gerando os Snapshots. 

**Os Snapshots criados pelos Iceberg possibilitam:**

1. **Viagem no Tempo (Time Travel):** Você pode consultar a tabela exatamente como ela existia em qualquer ponto do passado (por Timestamp ou por Snapshot ID). Se alguém pedir a foto exata do repositório no dia 11 de março às 14h, o Iceberg lê o snapshot daquele momento exato sem precisar duplicar dados.
2. **Isolamento de Leitura e Escrita (Snapshot Isolation):** Consultas no Trino e escritas no Spark acontecem ao mesmo tempo sem bloqueio de tabela (lock). Quem consulta lê o último snapshot validado; quando a escrita do Spark termina, um novo snapshot se torna ativo instantaneamente.
3. **Rollbacks Instantâneos e Segurança em Erros:** Se um job de transformação rodar com bugs ou com lote corrompido, é possível reverter a tabela para o snapshot anterior em milissegundos, pois os arquivos físicos antigos não são apagados imediatamente.
4. **Evolução Transparente de Schema e Particionamento:** Adicionar, renomear ou remover colunas não quebra as leituras dos arquivos antigos Parquet. O manifesto traduz o schema de cada snapshot sem exigir a reescrita de dados antigos.

**Seria possível treinar o uso dessas propriedados do 'snapshots' do Icerberg com esse projeto?**

Sim, é totalmente possível. Como o ambiente local já possui o Spark e o Trino conectados ao catálogo Iceberg, você pode usar essa mesma infraestrutura para explorar a tabela de metadados de snapshots, executar consultas de Time Travel (lendo a tabela como ela existia em momentos passados) e simular cenários de rollback via Spark SQL ou Trino.


**Sobre o`` DELETE FROM`` (Exclusão por lote):**

O que você disse: "seria excluir os arquivos .parquet relacionados ao ``batch_id``". Precisão técnica: Fisicamente, o Iceberg não apaga imediatamente os arquivos .parquet do disco (MinIO) no momento do ``DELETE``. Em vez disso, ele registra a exclusão nos arquivos de metadados/manifestos da tabela e gera um novo Snapshot. Para o usuário que faz um ``SELECT``, os dados sumiram (idempotência garantida). A remoção física dos arquivos do MinIO é realizada em um momento posterior através de rotinas de manutenção (Garbage Collection / Expire Snapshots).  

**Sobre o ``.append()`` (Persistência):**

Correto! O Spark escreve os novos arquivos .parquet na pasta de dados da tabela no MinIO e gera um novo manifesto que aponta para esses novos arquivos, consolidando o novo snapshot ativo.