"""Módulo responsável por rodar todo ETL de chat."""

import asyncio
import platform
import sys
import time
from datetime import datetime, timedelta

import nest_asyncio
from loguru import logger

from .ETL.manager_dbapi import upload_to_dbapi_chat
from .ETL.manager_dw import upload_to_dw_chat
from .ETL.manager_ods import upload_to_ods_chat
from .ETL.manager_stage import upload_to_stage_chat

nest_asyncio.apply()

if platform.platform()[:7] == "Windows":
    filepath = "c:/etl/five9/logs/Five9_Chat_ChamadasRecebidasAtivas.log"
else:
    filepath = "/var/bi-etl/five9/logs/Five9_Chat_ChamadasRecebidasAtivas.log"

logger.remove()
logger.add(
    sink=filepath,
    format="{time:MMMM D, YYYY > HH:mm:ss} | {level} | {name} | {message}",
    level="INFO",
    rotation="5 MB",
)


async def etl_pipeline(hours: int):
    """Executa a pipeline de ETL."""
    # Step 0: Start timer
    start_time = time.time()
    logger.info(f"ETL inicializado em {time.strftime('%X')}")

    folder_name = "Meus relatorios"
    reports_name = [
        "Report Chat - Interacoes Digitais",
        "Report Chat - Variaveis (Base para Cruzamento)",
    ]
    tables_stg = ["chamadas_chat", "base_variaveis_chat"]
    tables_dw = ["chamadas_recebidas_chat", "chamadas_ativas_chat"]
    startAt = (
        f"{datetime.isoformat(datetime.today() - timedelta(days=0, hours=hours))}"
        + "-03:00"
    )
    endAt = (
        f"{datetime.isoformat(datetime.today() - timedelta(days=0, hours=0))}"
        + "-03:00"
    )
    # startAt = "2024-01-02T00:00:00.000-03:00"
    # endAt = "2024-01-02T10:59:59.000-03:00"

    # Step 1: Extract data from Five9 and upload to DBAPI
    df_list = await upload_to_dbapi_chat(
        folder_name, reports_name, tables_stg, startAt, endAt
    )

    # Step 2: Transform data and upload to stage
    upload_to_stage_chat(df_list, tables_stg)

    # Step 3: Upload to DBDW
    upload_to_dw_chat(tables_dw)

    # Step 4: Upload to ODS
    upload_to_ods_chat(tables_dw)

    # Step 5: Stop timer and print total time
    end_time = time.time()
    logger.info(f"ETL finalizado em {time.strftime('%X')}")
    logger.info(f"Tempo total de execução: {(end_time - start_time):.2f} segundos")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        # Passe como arg a quantidade de horas que deseje obter de dados do report
        logger.error("Uso: python -m src.pipeline_chat horas")
        sys.exit(1)

    hours = int(
        sys.argv[1]
    )  # Obtenha a quantidade de horas de dados que o ETL irá buscar do report
    asyncio.run(etl_pipeline(hours))
