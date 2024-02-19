import json
import time
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine

conexao = (
    # Driver que será utilizado na conexão
    "DRIVER={ODBC Driver 17 for SQL Server};"
    # Host/IP ou nome do servidor
    "SERVER=10.15.16.52;"
    # Porta
    "PORT=1433;"
    # Banco que será utilizado
    "DATABASE=STAGE;"
    # Nome de usuário
    "UID=etl.bi;"
    # Senha
    "PWD=18JaTwkicdA!"
)

tempo_inicial = time.time()

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % quote_plus(conexao))
sql_query = """
    SELECT [id_contrato], [unidade_atendimento_cidades_atendimento]
    FROM [sydle].[stg_contrato]
    WHERE [unidade_atendimento_cidades_atendimento] IS NOT NULL"""

source_df = pd.read_sql(sql_query, engine)
salvar = []

for (
    id_contrato,
    unidade_atendimento_cidades_atendimento_str,
) in source_df.values.tolist():
    dados = json.loads(unidade_atendimento_cidades_atendimento_str)
    for unidade in dados:
        novo: dict = {
            "id_contrato": id_contrato,
            "nome": unidade["nome"],
            "codigo_ibge": unidade["codigo_ibge"],
        }
        salvar.append(novo)

df = pd.DataFrame(salvar)

df.to_sql(
    name="stg_contrato_unidade_atendimento",
    con=engine,
    if_exists="append",
    index=False,
    schema="sydle",
)

tempo_final = time.time()

print(f"o codigo demorou {int(tempo_final-tempo_inicial)} segundos para rodar")
