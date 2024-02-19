# Módulo de Extração dos Dados do Five9

Neste módulo é abordado a lógica de extração dos dados através de reports CSVs por meio de APIs. O método de download desses dados é através do protocolo HTTP. Por meio de uma biblioteca do python, foi possível realizar as requisições e obtenções dos reports necessários desse projeto. A forma genérica de utilização da biblioteca *httpx* pode ser vista [aqui](./httpx.md).

## Função que obtém o report no formato CSV
### ::: src.ETL.manager_five9.get_report_result_as_csv

## Função para obter o ID dos Reports
### ::: src.ETL.manager_five9.get_five9_identifier_report

## Função que verifica se o report está pronto para download
### ::: src.ETL.manager_five9.checks_report_is_running
