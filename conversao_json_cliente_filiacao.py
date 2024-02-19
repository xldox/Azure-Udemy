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
sql_query = """SELECT [id_cliente], [filiacao]
  FROM [sydle].[stg_cliente] where filiacao is not null and filiacao <> '[]'"""

source_df = pd.read_sql(sql_query, engine)
# print(source_df)
salvar = []

for id_cliente, filiacao_str in source_df.values.tolist():
    dados = json.loads(filiacao_str)
    for filiacao in dados:
        novo: dict = {'id_cliente': id_cliente,
                      'grau_parentesco': filiacao['grau_parentesco'],
                      'nome': filiacao['nome']}
        salvar.append(novo)

df = pd.DataFrame(salvar)
#
#df.info()
# # print(df.memory_usage(deep=True))

# print(df)
#
# df.to_csv("./nova_fiscal.csv", encoding="utf-8")
#
df.to_sql(name='stg_cliente_filiacao', con=engine, if_exists='append', index=False, schema='sydle')
#
tempo_final = time.time()
#
print(f"o codigo demorou {int(tempo_final-tempo_inicial)} segundos para rodar")

#
#
# with open('./dados.json', 'w' ) as fs:
    # json.dump(dados[0], fs, indent=4, ensure_ascii=False)