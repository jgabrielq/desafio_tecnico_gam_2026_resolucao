# Documentação Técnica: Script `bronze_events.py`

Este documento detalha o funcionamento e a arquitetura do job PySpark + Apache Iceberg responsável por carregar os eventos brutos da **Raw Zone** para a **Camada Bronze** do Lakehouse.

---

## 1. Visão Geral e Responsabilidade

O script `bronze_events.py` lê os arquivos comprimidos (`.json.gz` no formato NDJSON) armazenados na Raw Zone do MinIO (`raw/events/ingestion_date={ingestion_date}/`), aplica metadados de governança e persiste os dados na tabela Iceberg `lakehouse.bronze.events`.

* **Princípio da Camada Bronze:** Preservar a fidelidade aos dados brutos da fonte. Não realiza deduplicações nem regras de negócio (papel reservado à camada Silver).

---

## 2. Leitura do Arquivo e Tratamento de Schema Drift

1. **Leitura via Spark:** A função `run` recebe a data de ingestão (`ingestion_date`) e a sessão do Spark (`spark`), lendo os arquivos NDJSON diretamente da Raw Zone do MinIO.
2. **Tratamento do campo `properties`:**
   * Na leitura bruta via `spark.read.json()`, o Spark infere o campo `properties` como um objeto complexo (`StructType`).
   * Para evitar que quebras ocorram caso a API altere ou adicione novos campos (*schema drift*), a instrução `F.to_json(F.col("properties"))` reconverte essa estrutura em uma **string JSON pura (`STRING`)**.
   * Isso congela o schema do campo na Bronze e permite absorver alterações no payload da API sem interromper o pipeline.

---

## 3. Injeção de Colunas de Controle (Governança)

Antes de gravar na tabela, o DataFrame em memória recebe três colunas de controle identificadas pela convenção de underline (`_`):

* **`_ingested_at`** (`TIMESTAMP`): Timestamp UTC indicando o momento exato em que a transformação Bronze foi executada.
* **`_source_file`** (`STRING`): O caminho S3/MinIO exato do diretório bruto lido.
* **`_batch_id`** (`STRING`): Identificador determinístico do lote, equivalente ao `ingestion_date` (ex: `2026-03-11`).

---

## 4. Estruturação do Catálogo e Tabela Iceberg

1. **Garantia de Namespace:** O script executa a instrução `CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze` no Iceberg.
2. **Criação da Tabela:** Caso a tabela `lakehouse.bronze.events` não exista, ela é criada com tipagem majoritariamente em `STRING` (com exceção de `_ingested_at` em `TIMESTAMP`) para evitar falhas de conversão de dados da fonte.
3. **Formato de Armazenamento:** A tabela é configurada para salvar os dados em arquivos de formato colunar **Parquet** com compactação **Snappy**.

---

## 5. Garantia de Idempotência (Estratégia Delete-then-Append)

Para garantir que a reexecução do pipeline com o mesmo `--ingestion_date` não gere dados duplicados, o script adota a estratégia de sobrescrita por lote:

1. **Deleção Lógica:** Executa `DELETE FROM lakehouse.bronze.events WHERE _batch_id = '{ingestion_date}'`.
   * *Mecânica no Iceberg:* Não deleta fisicamente os arquivos Parquet do disco no momento da execução, mas atualiza a árvore de manifestos e gera um novo **Snapshot** no qual os registros correspondentes àquela data deixam de existir para consultas.
2. **Append:** Executa `df.writeTo("lakehouse.bronze.events").append()`, gravando os novos arquivos Parquet na pasta de dados da tabela no MinIO e atualizando o ponteiro ativo do catálogo.

---

## 6. Observabilidade e Validação

Ao final da execução, o script realiza uma consulta de contagem na tabela Iceberg:

```sql
SELECT COUNT(*) as total
FROM lakehouse.bronze.events
WHERE _batch_id = '{ingestion_date}'
```

---

## 7. O bloco ``if __name__ == "__main__":`` e o ``argparse``

Este bloco é o ponto de entrada do script quando ele é executado diretamente via linha de comando (ex: ``spark-submit transform/bronze_events.py --ingestion_date 2026-03-11``).

- ``if __name__ == "__main__":``: Garante que o código dentro dele só rodará se o arquivo for chamado diretamente no terminal. Se outro script apenas importar uma função deste arquivo (from transform.bronze_events import run), esse bloco é ignorado.
- ``argparse.ArgumentParser()``: Cria o analisador de argumentos de linha de comando.  
- ``add_argument("--ingestion_date", required=True)``: Define a flag ``--ingestion_date`` como obrigatória (``required=True``). Se o usuário rodar o comando sem passar a data, o Python interrompe a execução imediatamente e exibe uma mensagem no terminal explicando como usar o script.  
- ``args = parser.parse_args()``: Lê os argumentos digitados no terminal e extrai a variável (acessada em ``args.ingestion_date``) para passá-la como parâmetro na função ``run()``