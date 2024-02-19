import pandas as pd
import psycopg2
from parametros import windows as p
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

# Conectar ao banco de dados
conn = psycopg2.connect(
    database="datawarehouse",
    user="etl_bi",
    password="etl.bi",
    host="35.196.23.69"
)

# Definir a consulta SQL
sql_query = """
 SELECT
    cast(id_contrato as bigint) as id_contrato,
    cliente as NOME,
    email as EMAIL,
    tel_celular as CELULAR,
    tel_fixo as FIXO,
    documento as CPF,
    dim_canal as CANAL_DE_VENDA,
    plano as PLANO_CONTRATO,
    dim_cidade AS CIDADE,
    'VIP' as EMPRESA,
    CASE WHEN status_contrato IN ('HABILITADO','HABILITADO PARCIALMENTE','HABILITADO TEMPORARIAMENTE') THEN 1 ELSE 0 END as Contrato_ativo,
    data_contrato as dt_contratacao,
    data_ativacao,
    dim_estado,
    dim_cidade,
    dim_cep
FROM polo.dw_vip

UNION ALL

SELECT
    cast(id_contrato as bigint) as id_contrato,
    cliente as NOME,
    email as EMAIL,
    tel_celular as CELULAR,
    tel_fixo as FIXO,
    documento as CPF,
    dim_canal as CANAL_DE_VENDA,
    plano as PLANO_CONTRATO,
    dim_cidade AS CIDADE,
    'NIU' as EMPRESA,
    CASE WHEN status_contrato IN ('Ativo') THEN 1 ELSE 0 END as Contrato_ativo,
    data_contrato as dt_contratacao,
    data_ativacao,
    dim_estado,
    dim_cidade,
    dim_cep
FROM polo.dw_niu

UNION ALL

SELECT
    cast(id_contrato as bigint) as id_contrato,
    cliente as NOME,
    email as EMAIL,
    tel_celular as CELULAR,
    tel_fixo as FIXO,
    documento as CPF,
    dim_canal as CANAL_DE_VENDA,
    plano as PLANO_CONTRATO,
    dim_cidade AS CIDADE,
    'LIGUE' as EMPRESA,
    CASE WHEN tipo_cancelamento IN ('') THEN 1 ELSE 0 END as Contrato_ativo,
    data_contrato as dt_contratacao,
    data_ativacao,
    dim_estado,
    dim_cidade,
    dim_cep
FROM polo.dw_ligue;
"""

# Executar a consulta e armazenar o resultado em um DataFrame
df = pd.read_sql_query(sql_query, conn)

# Fechar a conexão
conn.close()

# Exibir as primeiras 100 linhas
print(df)
#df.to_csv('C:/Users/joao_psouza/Desktop/teste_teste.csv')
destination_table = 'contatos_vip'
engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % quote_plus(p))
Session = sessionmaker(bind=engine)
session = Session()
df.to_sql(destination_table, con=engine, schema='mailing', if_exists='append', index=False, chunksize=1000)