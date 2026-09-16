O que a Parte 4 envolve

Uma nova tabela Iceberg — lakehouse.quality.check_results — onde os resultados de cada verificação são persistidos (o enunciado é explícito: "não só impressos no terminal").

5+ checks com severidade e comportamento definidos:

Check	Onde verifica	Severidade
Unicidade da PK (event_id)	silver.events	BLOCKING — para o pipeline
Integridade referencial (customer_id → customers)	silver.events	WARNING — alerta
Volumetria fora do esperado	silver.events vs histórico	WARNING — alerta
Valores de domínio inválidos (event_type, plan)	Silver	WARNING — alerta
Freshness (dado mais recente dentro da janela)	silver.events	BLOCKING — para o pipeline

Um script Python quality/checks.py que roda os checks via Spark, persiste os resultados, e lança exceção se algum check BLOCKING falhar — interrompendo o pipeline downstream (Gold não roda se a Silver estiver corrompida).

O que NÃO envolve
Não cria Bronze nem Silver — só lê delas.
Não substitui os testes já existentes — complementa.
Não exige Airflow rodando — funciona standalone.