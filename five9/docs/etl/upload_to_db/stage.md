# Módulo de Upload no Banco de Dados STAGE

Neste módulo, temos as funções criadas que realizam o *input* dos dados brutos que são buscado através da API para as tabelas stages. Primeiramente, para realizar esse processo as tabelas auxiliares são alimentadas como é visto no código abaixo pela função [*load_data*](../manager_db.md). Por último, é executado as procedures para inserir os dados nas tabelas de stages. Para consultar o código SQL dessas procedures, basta clicar [aqui](./queries/procedures_stg.md).

## Tabelas Stage: Chamadas Voz
### ::: src.ETL.manager_stage.upload_to_stage_voz

## Tabelas Stage: Chamadas Chat
### ::: src.ETL.manager_stage.upload_to_stage_chat
