# Módulo de Upload no Banco de Dados DBAPI

Este módulo é o responsável por inserir os dados no banco de dados DBAPI. Esse é o banco no qual são salvos dados históricos em sua forma bruta. Os dados foram extraídos através da API disponibilizada pelo fornecedor e a lógica de captura desses dados pode ser visto [aqui](../extract.md). Neste projeto, foram feitas extrações de diversos reports inclusive de mídias distintas: voz e chat. No momento, o projeto tratou de reports com a seguinte característica: *chamadas recebidas voz*, *chamadas ativas voz*, *chamadas recebidas chat* e *chamadas ativas chat*.

## Chamadas Voz
### ::: src.ETL.manager_dbapi.upload_to_dbapi_voz

## Chamadas Chat
### ::: src.ETL.manager_dbapi.upload_to_dbapi_chat
