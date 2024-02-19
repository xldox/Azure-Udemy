import time
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy import text



conexao = (
    # Driver que será utilizado na conexão
    'DRIVER={ODBC Driver 17 for SQL Server};'
    # Host/IP ou nome do servidor
    'SERVER=10.15.16.52;'
    # Porta
    'PORT=1433;'
    # Banco que será utilizado
     'DATABASE=DBDW;'
    # Nome de usuário
     'UID=etl.bi;'
    # Senha
     'PWD=18JaTwkicdA!'
 )

tempo_inicial = time.time()


engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % quote_plus(conexao))
sql_query2 = (f"select max(DataHora) "
              f"from CentralAssinante_v2."
              f"chamados_app_novo;")
source_df = pd.read_sql(sql_query2, engine)

max_data_hora = source_df[""].iloc[0] if not source_df.empty else None
concate_data= "'"+str(max_data_hora)+"'"



sql_query3 = (f"SELECT CAST(E.session_id AS VARCHAR) + '_' + CAST(E.createdAt AS VARCHAR) AS IDChamado "
f",CAST(RIGHT(LEFT(E.createdat,10),4) + '-' + RIGHT(left(LEFT(E.createdat,10),5),2) + '-' + left(LEFT(E.createdat,10),2) + ' ' + RIGHT(E.createdAt,8) + '.000' AS DATETIME) AS DataHora "
f",FA.cidade collate SQL_Latin1_General_CP1_CI_AS as cidade "
f",FA.estado collate SQL_Latin1_General_CP1_CI_AS AS estado "
f",'' AS Classe "
f",C.nome collate SQL_Latin1_General_CP1_CI_AS AS Cliente "
f",F.document collate SQL_Latin1_General_CP1_CI_AS AS Documento "
f",FA.id_contrato AS Contrato "
f",'' AS Dispositivo "
f",os collate SQL_Latin1_General_CP1_CI_AS AS Plataforma "
f",F.source collate SQL_Latin1_General_CP1_CI_AS AS Origem "
f",label collate SQL_Latin1_General_CP1_CI_AS AS [Categoria Evento] "
f",title collate SQL_Latin1_General_CP1_CI_AS AS [Tipo Evento] "
f",UPPER(F.brand) collate SQL_Latin1_General_CP1_CI_AS AS Marca "
f",CAST(FA.data_ativacao AS VARCHAR) AS data_ativacao "
f",'V2' AS Versao "
f",E.session_id  "
f"FROM [DBDW].[app].[ft_events] E "
f"LEFT JOIN [DBDW].[app].[ft_sessions] F ON CAST(F.session_id AS VARCHAR) = CAST(E.session_id AS VARCHAR) "
f"LEFT JOIN (SELECT id_cliente,REPLACE(REPLACE(cpf,'.',''),'-','') AS cpf, REPLACE(REPLACE(cnpj,'.',''),'-','') AS cnpj,nome FROM DBDW.dbo.vw_dim_cliente) C ON CASE WHEN C.cpf IS NULL THEN C.cnpj else C.cpf END = F.document " 
f"LEFT JOIN (SELECT id_cliente, id_contrato FROM DBDW.dbo.vw_dim_contrato) CO ON CO.id_cliente = C.id_cliente "
f"LEFT JOIN (SELECT cidade,estado,id_contrato,data_ativacao FROM DBDW.dbo.vw_fato_ativacao) FA ON FA.id_contrato = CO.id_contrato "
f"WHERE CAST(RIGHT(LEFT(E.createdat, 10), 4) + '-' + RIGHT(left(LEFT(E.createdat, 10), 5), 2) + '-' + left(LEFT(E.createdat, 10), 2) + ' ' + RIGHT(E.createdAt, 8) + '.000' AS DATETIME)  > {concate_data};")

source_df2 = pd.read_sql(sql_query3, engine)


source_df2.to_sql('chamados_app_novo', con=engine, schema='CentralAssinante_v2', if_exists='append', index=False, chunksize=1000)




tempo_final = time.time()
tempo_execucao = tempo_final - tempo_inicial
print(f"Tempo de execução: {tempo_execucao} segundos")