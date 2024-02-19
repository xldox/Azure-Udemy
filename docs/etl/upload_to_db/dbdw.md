# Módulo de Upload no Banco de Dados DBDW

Neste módulo, temos as funções criadas que realizam o *input* dos dados que estão localizados no banco STAGE para as tabelas fato do banco de dados DBDW. Nesta etapa, também é feito consulta SQL com *joins* com as tabelas stages para gerar uma única tabela. A parte de carregamento dos dados é feita de forma incremental, ou seja, os dados que são inseridos nas tabelas fato são exclusivos. Para que esse processo ocorra, é realizado fazendo uma consulta na tabela fato e é feito a comparação do IDs entre essa consulta e os dados a serem inseridos. O resultado final será um DataFrame filtrado contendo apenas valores exclusivos.

## Tabelas Fato: Chamadas Voz
### ::: src.ETL.manager_dw.upload_to_dw_voz

## Tabelas Fato: Chamadas Chat
### ::: src.ETL.manager_dw.upload_to_dw_chat
