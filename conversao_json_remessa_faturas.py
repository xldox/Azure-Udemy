import json
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
sql_query = """SELECT [id_remessa], [faturas]
  FROM [sydle].[stg_remessa_integracao] where faturas is not null"""

source_df = pd.read_sql(sql_query, engine)
# print(source_df)
# print(source_df.values.tolist())

salvar = []

for id_remessa, faturas_str in source_df.values.tolist():
    dados = json.loads(faturas_str)
    for fatura in dados:
        novo: dict = {'id_remessa': id_remessa,
                      'fatura_codigo': fatura['fatura_codigo'],
                      'fatura_id': fatura['fatura_id']}
        salvar.append(novo)

df = pd.DataFrame(salvar)

# print(df)
#
# df.to_csv("./nova_fiscal.csv", encoding="utf-8")
#
df.to_sql(name='stg_remessa_faturas', con=engine, if_exists='append', index=False, schema='sydle')

tempo_final = time.time()

print(f"o codigo demorou {int(tempo_final-tempo_inicial)} segundos para rodar")

#
#
# with open('./dados.json', 'w' ) as fs:
    # json.dump(dados[0], fs, indent=4, ensure_ascii=False)

