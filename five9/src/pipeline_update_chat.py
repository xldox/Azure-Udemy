"""Módulo responsável por rodar todo ETL de chat."""

import asyncio
import platform
import time
from datetime import datetime, timedelta
from io import StringIO

import nest_asyncio
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from .ETL.manager_db import get_connection_from_db
from .ETL.manager_five9 import get_report_result_as_csv

nest_asyncio.apply()

if platform.platform()[:7] == "Windows":
    filepath = "c:/etl/five9/logs/Five9_Chat_UpdateChamadasRecebidasAtivas.log"
else:
    filepath = "/var/bi-etl/five9/logs/Five9_Chat_UpdateChamadasRecebidasAtivas.log"

logger.remove()
logger.add(
    sink=filepath,
    format="{time:MMMM D, YYYY > HH:mm:ss} | {level} | {name} | {message}",
    level="INFO",
    rotation="5 MB",
)


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
            f"Exceção não tratada ao carregar os dados da tabela {schema}.{table_name} para o banco de dados {database}. Motivo do erro: {e}"
        )
        pass

    session.commit()
    session.close()


async def download_report_five9(
    folder_name: str,
    report_name: str,
    table: str,
    startAt: str,
    endAt: str,
):
    """
    Função responsável por realizar o upload dos dados extraídos do Five9 nas tabelas de chat no banco DBAPI.

    Args:
        folder_name (str): Nome da pasta localizada na plataforma do Five9.
        reports_name (List[str]): Uma lista de nomes de reports.
        tables (List[str]): Uma lista de nomes das tabelas do banco de dados DBAPI.
        startAt (str): Data inicial desejada de extração.
        endAt (str): Data final desejada de extração.
    """
    engine, session = get_connection_from_db("STAGE")

    try:
        df = pd.read_csv(
            StringIO(
                await get_report_result_as_csv(folder_name, report_name, startAt, endAt)
            ),
            sep=",",
            dtype="object",
        )
        logger.info(
            f"Quantidade de dados baixados do {report_name}: {df['DATE'].count()}"
        )
    except Exception as e:
        logger.error(
            f"Falha ao tentar transformar em dataframe o {report_name}. Motivo do erro: {e}"
        )
        exit(1)

    df = df.astype(str)

    load_data(
        data=df,
        table_name=table,
        schema="five9",
        database="STAGE",
        engine=engine,
        session=session,
        if_exists="append",
    )


def updated_table_with_procedure():
    """Executa a procedure de update da tabela fato_chamadas_recebidas_chat."""
    engine, session = get_connection_from_db("STAGE")
    table_name = "fato_chamadas_recebidas_chat"

    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(text(f"EXEC STAGE.five9.proc_update_{table_name}"))
            conn.commit()
            logger.info(
                f"Procedure five9.proc_update_{table_name} do banco de dados STAGE foi executada com sucesso."
            )
    except Exception as e:
        logger.error(
            f"Falha na execução da procedure five9.proc_update_{table_name} do banco de dados STAGE. Motivo do erro: {e}"
        )
        exit(1)


async def etl_pipeline():
    """Executa a pipeline de ETL."""
    # Step 0: Start timer
    start_time = time.time()
    logger.info(f"ETL inicializado em {time.strftime('%X')}")

    # Step 1: Download dos arquivos
    folder_name = "Meus relatorios"
    report_table_dict = {
        "Report Chat - Interacoes Digitais": "chamadas_chat_to_update",
    }
    qtd_dias = 7
    for report_name, table in report_table_dict.items():
        for i in range(qtd_dias):
            startAt = (
                f"{datetime.isoformat((datetime.today() - timedelta(days=qtd_dias - i)).replace(hour=0, minute=0, second=0, microsecond=0))}"
                + "-03:00"
            )
            endAt = (
                f"{datetime.isoformat((datetime.today() - timedelta(days=qtd_dias - (i+1))).replace(hour=0, minute=0, second=0, microsecond=0))}"
                + "-03:00"
            )
            # Step 1: Extract historical data from Five9 and upload to aux table to update columns of prod tables
            await download_report_five9(folder_name, report_name, table, startAt, endAt)

    # Step 2: Executa a procedure para realizar as atualizações necessárias na tabela fato de chamadas_recebidas_chat
    updated_table_with_procedure()

    # Step 3: Stop timer and print total time
    end_time = time.time()
    logger.info(f"ETL finalizado em {time.strftime('%X')}")
    logger.info(f"Tempo total de execução: {(end_time - start_time):.2f} segundos")


if __name__ == "__main__":
    asyncio.run(etl_pipeline())
