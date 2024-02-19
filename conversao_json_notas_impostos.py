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
sql_query = """SELECT  [id_nota_fiscal], [itens]
  FROM [sydle].[stg_notas_fiscais] """

source_df = pd.read_sql(sql_query, engine)
# print(source_df)
salvar = []

# for id_nota_fiscal, item_str in source_df.values.tolist():
#     dados = json.loads(item_str)
#     for item in dados:
#         for produto in item['produtos']:
#             for imposto in item['impostos']:
#                 novo: dict = {'id_nota_fiscal': id_nota_fiscal,
#                               'nome_produto': produto['nome'],
#                               'valor_base': item['valor_base'],
#                               'impostos_retidos': item['impostos_retidos'],
#                               'servico_tributavel_id': item['servico_tributavel_id'],
#                               'valor_total_dos_impostos': item['valor_total_dos_impostos'],
#                               'servico_tributavel_nome': item['servico_tributavel_nome'],
#                               'base_de_calculo': imposto['base_de_calculo'],
#                               'aliquota': imposto['aliquota'],
#                               'imposto': imposto['imposto'],
#                               'valor': imposto['valor'],
#                               'retido': imposto['retido'],
#                               'Exigibilidade': imposto['Exigibilidade']}
#                 salvar.append(novo)


for id_nota_fiscal, item_str in source_df.values.tolist():
    dados = json.loads(item_str)
    for item in dados:
        for imposto in item['impostos']:
            novo: dict = {'id_nota_fiscal': id_nota_fiscal,
                          'descricao_produto': item['descricao'],
                          'valor_total_dos_impostos_do_produto': item['valor_total_dos_impostos'],
                          'imposto': imposto['imposto'],
                          'base_de_calculo': imposto['base_de_calculo'],
                          'aliquota': imposto['aliquota'],
                          'valor': imposto['valor'],
                          'retido': imposto['retido'],
                          'impostos_retidos': item['impostos_retidos'],
                          'Exigibilidade': imposto['Exigibilidade']}
            salvar.append(novo)

df = pd.DataFrame(salvar)
#
# #df.info()
# # print(df.memory_usage(deep=True))
#print(df)
#
# df.to_csv("./nova_fiscal.csv", encoding="utf-8")
#
#df.to_sql(name='stg_nota_fiscal_impostos', con=engine, if_exists='append', index=False, schema='sydle')
#
tempo_final = time.time()
#
print(f"o codigo demorou {int(tempo_final - tempo_inicial)} segundos para rodar")

#
#
# with open('./dados.json', 'w' ) as fs:
# json.dump(dados[0], fs, indent=4, ensure_ascii=False)