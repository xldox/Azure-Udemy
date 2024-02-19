"""Módulo para gerenciar os databases do SQL Server."""

import platform
from typing import Tuple
from urllib.parse import quote_plus

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


def get_connection_from_db(database: str) -> Tuple[create_engine, Session]:
    """
    Cria o engine e a session para realizar as conexões ao banco de dados.

    Args:
        database (str): Nome do banco de dados.

    Returns:
        tuple[Engine, Session]: Tupla dos objetos Engine e Session.
    """
    if database == "DBAPI":
        if platform.platform()[:7] == "Windows":
            from .connect_string import windows_dbapi as p
        else:
            from .connect_string import ubuntu_dbapi as p
    elif database == "STAGE":
        if platform.platform()[:7] == "Windows":
            from .connect_string import windows_stg as p
        else:
            from .connect_string import ubuntu_stg as p
    elif database == "DBDW":
        if platform.platform()[:7] == "Windows":
            from .connect_string import windows_dw as p
        else:
            from .connect_string import ubuntu_dw as p
    else:
        if platform.platform()[:7] == "Windows":
            from .connect_string import windows_ods as p
        else:
            from .connect_string import ubuntu_ods as p

    # Create a connection with DW
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % quote_plus(p))
    Session = sessionmaker(bind=engine)
    session = Session()

    return engine, session


def load_data(
    data: pd.DataFrame,
    table_name: str,
    schema: str,
    database: str,
    engine: create_engine,
    session: Session,
    if_exists: str = "append",
) -> None:
    """
    Carrega dados do dataframe para tabelas no banco de dados.

    Args:
        data (DataFrame): DataFrame contendo os dados a serem carregados na tabela.
        table_name (str): Nome da tabela do banco de dados.
        schema (str): Nome do schema criado dentro do banco de dados.
        database (str): Nome do banco de dados.
        engine (create_engine): Mecanismo de comunicação entre o SQLAlchemy e o banco de dados.
        session (Session): Sessão de comunicação entre os objetos Python e o engine.
        if_exists (str): Modo que a função irá se comportar se existir a tabela.
            'fail': Retorna um erro.
            'replace': Deleta a tabela existente e cria uma nova.
            'append': Insere novas linhas sem apagar as existentes.

    Returns:
        None
    """
    try:
        data.to_sql(
            name=table_name, con=engine, schema=schema, if_exists=if_exists, index=False
        )
        logger.info(
            f"Dados da tabela {schema}.{table_name} carregados com sucesso para o banco de dados {database}."
        )
    except Exception as e:
        logger.error(
            f"Falha ao carregar os dados da tabela {schema}.{table_name} para o banco de dados {database}. Motivo do erro: {e}"
        )
        exit(1)

    session.commit()
    session.close()
