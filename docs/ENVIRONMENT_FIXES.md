# Ajustes no ambiente (WSL2 + Docker)

Registro das mudanças feitas para conseguir subir o ambiente e do problema de consumo de memória identificado no WSL2, com a explicação técnica de cada uma.

## Contexto

- Repositório aberto dentro do WSL2 (Windows), com apenas 16 GB de RAM na máquina host.
- Sem `.wslconfig` customizado, o WSL2 por padrão usa no máximo 50% da RAM do Windows (ou 8 GB, o que for menor) — nesta máquina a VM do WSL2 tinha **7,7 GiB** de RAM totais, não os 16 GB do host.
- Ao rodar `make up` pela primeira vez, dois problemas apareceram: (1) o ambiente não subia (falha no `minio-init`) e (2) mesmo antes de todos os serviços estarem no ar, o consumo de memória da VM (`Vmmem`, visto no Gerenciador de Tarefas do Windows) já chegava a ~92% de uso (~8,2 GB).

---

## 1. Falha no `minio-init` — arquivos `.sh` com CRLF

**Sintoma:**
```
dl-minio-init  | /scripts/minio-init.sh: line 2: set: -: invalid option
```
O container `minio-init` (responsável por criar os buckets no MinIO) falhava com exit code 2, e isso travava toda a cadeia de `depends_on`: `iceberg-rest`, `trino` e `spark` nunca chegavam a iniciar (ficavam parados em estado `Created`).

**Causa:**
Todos os arquivos em `scripts/*.sh` estavam salvos com terminadores de linha **CRLF** (Windows) em vez de **LF** (Unix) — provável efeito de editar/clonar o repositório em ambiente Windows/WSL sem `core.autocrlf` configurado corretamente. O `/bin/sh` (dash) do container `minio/mc` não tolera o `\r` residual na linha `set -e`, e quebra a interpretação do script inteiro.

**Correção:**
Convertidos os terminadores de linha de CRLF para LF em todos os scripts:
```bash
for f in scripts/*.sh; do sed -i 's/\r$//' "$f"; done
```
Arquivos afetados:
- `scripts/minio-init.sh`
- `scripts/diag_s3a.sh`
- `scripts/diag_spark_image.sh`
- `scripts/diag_trino.sh`
- `scripts/verify_env.sh`

Nenhuma linha de conteúdo foi alterada — apenas o caractere de fim de linha.

---

## 2. Consumo excessivo de memória (Vmmem alto no WSL2)

**Sintoma:**
Gerenciador de Tarefas do Windows reportando ~92% de uso de RAM (`Vmmem` em ~8,2 GB), mesmo com o ambiente ainda subindo.

**Causa:**
Nenhum serviço no `docker-compose.yml` tinha limite de memória definido. O maior problema estava nas JVMs:

- **Trino** (imagem `trinodb/trino:450`) usa por padrão as flags:
  ```
  -XX:InitialRAMPercentage=80
  -XX:MaxRAMPercentage=80
  ```
  Sem um limite de memória no container (cgroup), a JVM calcula esse percentual sobre a memória **total visível do host** — nesse caso, os 7,7 GiB inteiros da VM do WSL2. Ou seja, o Trino tentava reservar sozinho até **~6,2 GB** de heap para processar um dataset de alguns milhares de linhas.
- **Iceberg REST** (`tabulario/iceberg-rest`) não define `-Xmx` explícito; o default da JVM (Java 17) é usar 1/4 da memória visível — de novo, calculado sobre o host inteiro sem um limite de container.
- **Spark** já definia `spark.driver.memory 2g` em `conf/spark/spark-defaults.conf`, então esse processo já era relativamente contido, mas ainda sem um teto de container que evitasse crescimento do processo como um todo (overhead de JVM, PySpark, etc.).

Esse comportamento — heap dimensionado como fração da RAM do host, e não do que o processo de fato precisa — é a causa direta do consumo alto reportado no Vmmem.

**Correção:**
Adicionado `mem_limit` para cada serviço em `docker-compose.yml`. JVMs modernas (Java 10+) são *cgroup-aware*: ao existir um limite de memória no container, `-XX:MaxRAMPercentage` e o default de heap passam a ser calculados sobre esse limite, e não sobre a RAM total do host.

| Serviço | `mem_limit` definido | Motivo |
|---|---|---|
| `minio` | 512m | Object store leve, uso real ~100 MB |
| `minio-init` | 128m | Script de setup, roda e encerra |
| `iceberg-rest` | 768m | JVM sem `-Xmx`; heap passa a ser 1/4 de 768m (~192 MB) em vez de 1/4 da VM inteira |
| `spark` | 3g | Cobre o `spark.driver.memory=2g` já configurado + overhead de JVM/Python/Jupyter |
| `trino` | 1536m | `MaxRAMPercentage=80` passa a calcular 80% de 1,5 GiB (~1,2 GB de heap) em vez de 80% da VM |
| `postgres` | 512m | Uso real ~30-60 MB para esse volume de dados |
| `mock-api` | 256m | Processo Python/uvicorn leve |
| `airflow` (perfil opcional) | 1536m | Mesmo raciocínio, usado apenas com `make airflow` |

Nenhum desses limites reduz funcionalidade: todos foram definidos com folga acima do uso real observado em `docker stats` depois de rodar `make test-infra` (round-trip completo Spark → Iceberg → MinIO → Trino).

---

---

## 3. Trava de segurança contra falta de memória (`mem_swappiness` + `restart`)

**Pergunta que motivou este passo:** os `mem_limit` da seção 2 reduzem o consumo de cada serviço, mas **a soma** dos limites (~6,6 GiB, sem contar o `airflow`) ainda está próxima do teto de 7,7 GiB da VM do WSL2. Existe risco de faltar memória?

**Análise do risco:**
- O Windows **não corre risco de travar por causa disso**: o WSL2 (e o Docker Desktop, que roda sob o mesmo teto de memória compartilhado da VM) já tem um limite rígido de memória total, imposto fora do repositório. Nenhum processo dentro dos containers consegue crescer além desse teto e consumir RAM do host Windows.
- O risco real é **dentro da VM do WSL2**: por padrão, o Docker permite que um container ultrapasse seu `mem_limit` recorrendo à *swap* (o padrão é até 2x o `mem_limit` em swap). Isso não gera uma falha limpa — gera **thrashing**: o container fica lento, e por extensão todo o WSL2 fica lento, antes que o kernel decida encerrar algum processo. Esse é o cenário mais próximo de "quebrar a máquina local" na prática.

**Correção:**
Adicionado a todos os serviços (exceto `minio-init`, que é uma tarefa única de setup):
```yaml
mem_swappiness: 0
restart: on-failure
```
- `mem_swappiness: 0` impede que aquele container específico recorra à swap. Ao atingir o `mem_limit`, o kernel encerra o processo de forma limpa e imediata (`OOMKilled`) em vez de deixar o sistema todo degradar lentamente.
- `restart: on-failure` faz o Compose reiniciar automaticamente qualquer serviço que seja encerrado por esse motivo, em vez de deixar o pipeline travado silenciosamente.

Nenhuma configuração do Windows/WSL2 foi tocada — a trava é inteiramente definida no `docker-compose.yml`.

**Validação:**
Depois de recriar os containers com essa configuração, rodei `make test-infra` — o teste **completo**, que exercita justamente a carga mais pesada (tabela particionada, `MERGE INTO`, tags/time travel, evolução de schema, leitura cruzada Spark/Trino). Resultado: **46/46 verificações OK**, e nenhum container foi `OOMKilled`:

| Serviço | Uso no pico do teste | Limite | Margem |
|---|---|---|---|
| `dl-trino` | 1,08 GiB | 1,5 GiB | 71,8% do limite |
| `dl-spark` | 1012 MiB | 3 GiB | 32,9% do limite |
| `dl-iceberg-rest` | 232 MiB | 768 MiB | 30,2% do limite |
| `dl-mock-api` | 79 MiB | 256 MiB | 31,1% do limite |
| `dl-minio` | 96 MiB | 512 MiB | 18,8% do limite |
| `dl-postgres` | 32 MiB | 512 MiB | 6,3% do limite |

WSL2 (`free -h`) ao final: 4,7 GiB usados / 7,7 GiB totais, 3,0 GiB disponíveis — mesmo no cenário de carga mais pesada previsto pelo desafio.

**Observação sobre o perfil `airflow`:** ele não entra nessa soma por padrão (só sobe com `make airflow`). Se for usado *simultaneamente* a um `make test-infra`, a soma de limites ativos passa a ~8,1 GiB, acima do teto da VM — nesse caso a trava (`mem_swappiness: 0`) ainda evita o thrashing generalizado, mas o serviço que estourar primeiro pode ser reiniciado em loop. Recomenda-se não rodar o Airflow em paralelo com testes pesados de Spark/Trino.

---

## Resultado

Depois das três correções, `make up`, `make check` e `make test-infra` passam com sucesso:
- `make check`: **12/12 verificações OK**, incluindo o teste de escrita ponta a ponta.
- `make test-infra`: **46/46 verificações OK**, incluindo MERGE INTO, partições, time travel e evolução de schema — sem nenhum container `OOMKilled`.

Comparativo de memória (medido com `free -h` dentro do WSL2 e `docker stats`):

| Métrica | Antes | Depois |
|---|---|---|
| RAM usada na VM do WSL2 (`free -h`) | ~92% reportado no Windows (Vmmem ~8,2 GB) | 4,3 GiB usados / 7,7 GiB totais (3,4 GiB disponíveis) |
| Uso real somado dos containers (`docker stats`) | N/A (Trino/Spark/Iceberg REST nunca chegavam a subir, por causa da falha no `minio-init`) | ~2,5 GiB no total, todos abaixo dos seus `mem_limit` |
| Trino | heap potencial de até ~6,2 GB (80% de 7,7 GiB) | 977 MiB usados, limite de 1,5 GiB |
| Iceberg REST | heap potencial de ~1,9 GB (1/4 de 7,7 GiB) | 225 MiB usados, limite de 768 MiB |

## Arquivos alterados

- `scripts/minio-init.sh`, `scripts/diag_s3a.sh`, `scripts/diag_spark_image.sh`, `scripts/diag_trino.sh`, `scripts/verify_env.sh` — normalização de fim de linha (CRLF → LF).
- `docker-compose.yml` — adição de `mem_limit`, `mem_swappiness: 0` e `restart: on-failure` em todos os serviços (exceto `minio-init`, tarefa de setup de execução única).

## Fora de escopo (não aplicado)

Ficou fora do repositório, por preferência do usuário, um ajuste do lado do Windows que também ajudaria a memória geral do WSL2: definir `memory=` em `C:\Users\<usuário>\.wslconfig` (hoje o WSL2 usa o default de 50% da RAM do Windows, isto é, ~8 GB de um total de 16 GB) e reiniciar com `wsl --shutdown`. Esse passo aumentaria o teto disponível para a VM inteira, mas não foi necessário depois dos ajustes acima — o ambiente já roda confortavelmente dentro dos 7,7 GiB atuais.
