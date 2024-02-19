"""Módulo responsável por rodar todo ETL de checkpoints voz."""

import asyncio
import platform
import sys
import time
from datetime import datetime, timedelta

import nest_asyncio
from loguru import logger

from .ETL.manager_dbapi import upload_checkpoints_voz_to_dbapi
from .ETL.manager_dw import upload_checkpoints_voz_to_dw
from .ETL.manager_stage import upload_checkpoints_voz_to_stage

# from .ETL.manager_ods import upload_to_ods_voz

nest_asyncio.apply()

if platform.platform()[:7] == "Windows":
    filepath = "c:/etl/five9/logs/Five9_Voz_Checkpoints.log"
else:
    filepath = "/var/bi-etl/five9/logs/Five9_Voz_Checkpoints.log"

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
    report_name = "Report - Checkpoints"
    table = "checkpoints_voz"
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
    df = await upload_checkpoints_voz_to_dbapi(
        folder_name, report_name, table, startAt, endAt
    )

    # Step 2: Transform data and upload to stage
    upload_checkpoints_voz_to_stage(df, table)

    # Step 3: Upload to DBDW
    upload_checkpoints_voz_to_dw(table)

    # Step 4: Upload to ODS
    # await upload_to_ods_voz(table, hours)

    # Step 5: Stop timer and print total time
    end_time = time.time()
    logger.info(f"ETL finalizado em {time.strftime('%X')}")
    logger.info(f"Tempo total de execução: {(end_time - start_time):.2f} segundos")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        # Passe como arg a quantidade de horas que deseje obter de dados do report
        logger.error("Uso: python -m src.pipeline_checkpoints_voz horas")
        sys.exit(1)

    hours = int(
        sys.argv[1]
    )  # Obtenha a quantidade de horas de dados que o ETL irá buscar do report
    asyncio.run(etl_pipeline(hours))
