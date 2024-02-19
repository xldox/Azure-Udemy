import pandas as pd
import mysql.connector as mysql
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Conectar ao banco de dados MySQL
conn_mysql = mysql.connect(host='10.15.16.30',
                           database='air_comercial',
                           user='etl.bi',
                           password='refAiREQuish')

# Definir a consulta SQL para MySQL
sql_query_mysql = """
SELECT DISTINCT id FROM tbl_cliente;
"""

# Executar a consulta e armazenar o resultado em um DataFrame
df_mysql = pd.read_sql_query(sql_query_mysql, conn_mysql)

# Fechar a conexão com o MySQL
conn_mysql.close()

# Conectar ao banco de dados SQL Server
conn_sql_server = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=10.15.16.52;'
    'PORT=1433;'
    'DATABASE=DBDW;'
    'UID=etl.bi;'
    'PWD=18JaTwkicdA!'
)

# Criar engine para SQL Server
engine_sql_server = create_engine(f'mssql+pyodbc:///?odbc_connect={quote_plus(conn_sql_server)}')

# Definir a consulta SQL para SQL Server
sql_query_sql_server = """
SELECT DISTINCT id FROM dim_cliente;
"""

# Executar a consulta e armazenar o resultado em um DataFrame
df_sql_server = pd.read_sql_query(sql_query_sql_server, engine_sql_server)

# Converter a coluna 'id' para o mesmo tipo de dados (object) em ambos os DataFrames
df_mysql['id'] = df_mysql['id'].astype(str)
df_sql_server['id'] = df_sql_server['id'].astype(str)

#print('MYSQL',df_mysql)
#print('SQL',df_sql_server)

# Realizar a junção (LEFT JOIN) entre os DataFrames usando a coluna 'id' como chave de junção
#result = pd.merge(df_mysql,df_sql_server, on='id', how='left')
result = pd.merge(df_mysql, df_sql_server, on='id', how='left', indicator=True)

ids_not_in_sql_server = result[result['_merge'] == 'left_only']['id']


# Exibir o resultado da junção
#print(result)
print(ids_not_in_sql_server)
ids_not_in_sql_server.to_csv('C:/Users/joao_psouza/Desktop/teste.csv')