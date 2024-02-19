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
sql_query = """SELECT TOP (100) [id_contrato], [unidade_atendimento_cidades_atendimento]
  FROM [sydle].[stg_contrato]"""




source_df = pd.read_sql(sql_query, engine)
print(source_df)
salvar = []


for id_contrato, cidade in source_df.values.tolist():
    try:
        dados = json.loads(cidade)
        for cidade in dados:
            novo: dict = {'id_contrato': id_contrato,
                          'nome': cidade['nome'],
                          'codigo_ibge': cidade['codigo_ibge']
                          }
            salvar.append(novo)
    except:
            novo: dict = {'id_contrato': id_contrato,
                          'nome': '',
                          'codigo_ibge':''}
            salvar.append(novo)


df = pd.DataFrame(salvar)
#
df.info()
# # print(df.memory_usage(deep=True))

print(df)

#df.to_csv("C:/Users/João Paulo Souza/Desktop/cidades.csv", encoding='utf-8-sig')




#
# df.to_csv("./nova_fiscal.csv", encoding="utf-8")
#
# df.to_sql(name='stg_contratos_componentes', con=engine, if_exists='replace', index=False, schema='sydle')
#
tempo_final = time.time()
#
print(f"o codigo demorou {int(tempo_final-tempo_inicial)} segundos para rodar")

#
#
# with open('./dados.json', 'w' ) as fs:
    # json.dump(dados[0], fs, indent=4, ensure_ascii=False)