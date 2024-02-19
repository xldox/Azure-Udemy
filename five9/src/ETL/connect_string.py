"""Módulo responsável por guardar as credenciais dos bancos de dados."""

import os

from dotenv import load_dotenv

load_dotenv()

# String de conexão Windows Server
windows_dbapi = (
    # Driver que será utilizado na conexão
    "DRIVER={ODBC Driver 17 for SQL Server};"
    # Host/IP ou nome do servidor de produção
    "SERVER="
    + os.environ["SERVER_DEV"]
    + ";"
    # Porta
    "PORT=1433;"
    # Banco que será utilizado
    "DATABASE=DBAPI;"
    # Nome de usuário
    "UID="
    + os.environ["UID_DEV"]
    + ";"
    # Senha
    "PWD="
    + os.environ["PWD_DEV"]
)

windows_stg = (
    # Driver que será utilizado na conexão
    "DRIVER={ODBC Driver 17 for SQL Server};"
    # Host/IP ou nome do servidor de produção
    "SERVER="
    + os.environ["SERVER_DEV"]
    + ";"
    # Porta
    "PORT=1433;"
    # Banco que será utilizado
    "DATABASE=STAGE;"
    # Nome de usuário
    "UID="
    + os.environ["UID_DEV"]
    + ";"
    # Senha
    "PWD="
    + os.environ["PWD_DEV"]
)

windows_dw = (
    # Driver que será utilizado na conexão
    "DRIVER={ODBC Driver 17 for SQL Server};"
    # Host/IP ou nome do servidor de produção
    "SERVER="
    + os.environ["SERVER_DEV"]
    + ";"
    # Porta
    "PORT=1433;"
    # Banco que será utilizado
    "DATABASE=DBDW;"
    # Nome de usuário
    "UID="
    + os.environ["UID_DEV"]
    + ";"
    # Senha
    "PWD="
    + os.environ["PWD_DEV"]
)

windows_ods = (
    # Driver que será utilizado na conexão
    "DRIVER={ODBC Driver 17 for SQL Server};"
    # Host/IP ou nome do servidor de produção
    "SERVER="
    + os.environ["SERVER_ODS_DEV"]
    + ";"
    # Porta
    "PORT=1433;"
    # Banco que será utilizado
    "DATABASE=ODS;"
    # Nome de usuário
    "UID="
    + os.environ["UID_ODS_DEV"]
    + ";"
    # Senha
    "PWD="
    + os.environ["PWD_ODS_DEV"]
)

# String de conexão Ubuntu
ubuntu_dbapi = (
    # Driver que será utilizado na conexão
    "DRIVER={ODBC Driver 17 for SQL Server};"
    # Host/IP ou nome do servidor\Versão do SQL
    "SERVER="
    + os.environ["SERVER_PROD"]
    + ";"
    # Porta
    "PORT=1433;"
    # Banco que será utilizado
    "DATABASE=DBAPI;"
    # Nome de usuário
    "UID="
    + os.environ["UID_PROD"]
    + ";"
    # Senha
    "PWD="
    + os.environ["PWD_PROD"]
)

ubuntu_stg = (
    # Driver que será utilizado na conexão
    "DRIVER={ODBC Driver 17 for SQL Server};"
    # Host/IP ou nome do servidor\Versão do SQL
    "SERVER="
    + os.environ["SERVER_PROD"]
    + ";"
    # Porta
    "PORT=1433;"
    # Banco que será utilizado
    "DATABASE=STAGE;"
    # Nome de usuário
    "UID="
    + os.environ["UID_PROD"]
    + ";"
    # Senha
    "PWD="
    + os.environ["PWD_PROD"]
)

ubuntu_dw = (
    # Driver que será utilizado na conexão
    "DRIVER={ODBC Driver 17 for SQL Server};"
    # Host/IP ou nome do servidor\Versão do SQL
    "SERVER="
    + os.environ["SERVER_PROD"]
    + ";"
    # Porta
    "PORT=1433;"
    # Banco que será utilizado
    "DATABASE=DBDW;"
    # Nome de usuário
    "UID="
    + os.environ["UID_PROD"]
    + ";"
    # Senha
    "PWD="
    + os.environ["PWD_PROD"]
)

ubuntu_ods = (
    # Driver que será utilizado na conexão
    "DRIVER={ODBC Driver 17 for SQL Server};"
    # Host/IP ou nome do servidor\Versão do SQL
    "SERVER="
    + os.environ["SERVER_ODS_PROD"]
    + ";"
    # Porta
    "PORT=1433;"
    # Banco que será utilizado
    "DATABASE=ODS;"
    # Nome de usuário
    "UID="
    + os.environ["UID_ODS_PROD"]
    + ";"
    # Senha
    "PWD="
    + os.environ["PWD_ODS_PROD"]
)
