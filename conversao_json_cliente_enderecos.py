import json
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import time


conexao = (
    # Driver que será utilizado na conexão
    'DRIVER={ODBC Driver 17 for SQL Server};'
    # Host/IP ou nome do servidor
    'SERVER=10.15.16.52;'
    # Porta
    'PORT=1433;'
    # Banco que será utilizado
     'DATABASE=STAGE;'
    # Nome de usuário
     'UID=etl.bi;'
    # Senha
     'PWD=18JaTwkicdA!'
 )

tempo_inicial = time.time()

engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % quote_plus(conexao))
sql_query = """SELECT [id_cliente], [enderecos]
  FROM [sydle].[stg_cliente] where enderecos is not null and enderecos <> '[]'"""

source_df = pd.read_sql(sql_query, engine)
# print(source_df)
salvar = []

for id_cliente, enderecos_str in source_df.values.tolist():
    dados = json.loads(enderecos_str)
    for enderecos in dados:
        novo: dict = {'id_cliente': id_cliente,
                      'cidade_nome': enderecos['cidade_nome'],
                      'tipo': enderecos['tipo'],
                      'complemento': enderecos['complemento'],
                      'numero': enderecos['numero'],
                      'logradouro': enderecos['logradouro'],
                      'bairro': enderecos['bairro'],
                      'estado_nome': enderecos['estado_nome'],
                      'referencia': enderecos['referencia'],
                      'cidade_codigo_ibge': enderecos['cidade_codigo_ibge'],
                      'estado_sigla': enderecos['estado_sigla'],
                      'cep': enderecos['cep'],
                      'pais': enderecos['pais']}
        salvar.append(novo)

df = pd.DataFrame(salvar)
#
#df.info()
# # print(df.memory_usage(deep=True))

# print(df)
#
# df.to_csv("./nova_fiscal.csv", encoding="utf-8")
#
df.to_sql(name='stg_cliente_enderecos', con=engine, if_exists='append', index=False, schema='sydle')
#
tempo_final = time.time()
#
print(f"o codigo demorou {int(tempo_final-tempo_inicial)} segundos para rodar")

#
#
# with open('./dados.json', 'w' ) as fs:
    # json.dump(dados[0], fs, indent=4, ensure_ascii=False)