# Módulo de Upload no Banco de Dados ODS

Neste módulo, diferentemente da carga no banco de dados DBDW, não é mais necessário realizar consultas SQL ao banco de dados STAGE. Para carregar os dados para as tabelas ODS, é feito apenas consultas as tabelas fato do banco DBDW com filtro nas últimas horas de dados inseridos. Para a carga incremental, realiza-se o mesmo processo de comparação de IDs como visto no [módulo DBDW](./dbdw.md).

## Tabelas Fato: Chamadas Voz
### ::: src.ETL.manager_ods.upload_to_ods_voz

## Tabelas Fato: Chamadas Chat
### ::: src.ETL.manager_ods.upload_to_ods_chat
