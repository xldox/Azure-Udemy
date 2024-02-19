import os
from dotenv import load_dotenv

load_dotenv()

# String de conexão Windows Server
windows = (
    # Driver que será utilizado na conexão 
    'DRIVER={ODBC Driver 17 for SQL Server};'
    # Host/IP ou nome do servidor de produção
    'SERVER=' + os.environ['SERVER_DEV'] + ';'
    # Porta
    'PORT=1433;'
    # Banco que será utilizado
     'DATABASE=DBAPI;'
    # Nome de usuário
     'UID=' + os.environ['UID_DEV'] + ';'
    # Senha
     'PWD=' + os.environ['PWD_DEV']
 )

# String de conexão Ubuntu
ubuntu = (
    # Driver que será utilizado na conexão
    'DRIVER={ODBC Driver 17 for SQL Server};'
    # Host/IP ou nome do servidor\Versão do SQL
    'SERVER=' + os.environ['SERVER_PROD'] + ';'
    # Porta
    'PORT=1433;'
    # Banco que será utilizado
    'DATABASE=DBAPI;'
    # Nome de usuário
    'UID=' + os.environ['UID_PROD'] + ';'
    # Senha
    'PWD=' + os.environ['PWD_PROD']
)


