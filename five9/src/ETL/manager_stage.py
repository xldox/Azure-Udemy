"""Esse módulo é o responsável pelos inputs no banco de dados STAGE."""

from datetime import datetime, timedelta
from typing import List

import pandas as pd
from loguru import logger
from sqlalchemy import text

from .manager_db import get_connection_from_db, load_data
from .manager_dbapi import upload_to_dbapi_chat, upload_to_dbapi_voz


def convert_datetime_with_timezone(timestamp: str) -> datetime:
    """
    Converta o valor de uma célula da coluna 'MODULE START TIMESTAMP' para um formato de data e hora específico.

    Args:
        timestamp (str): Valor da célula a ser processado, no formato original "%a, %d %b %Y %H:%M:%S".

    Returns:
        datetime: O registro formatado como datetime, com um ajuste de 4 horas ao fuso horário.

    Exemplo:
        >>> timestamp = "Tue, 10 Oct 2023 18:19:40"
        >>> result = convert_datetime_with_timezone(timestamp)
        >>> print(result)
        "2023-10-10 22:19:40"
    """
    converted_datetime = datetime.strptime(
        timestamp, "%a, %d %b %Y %H:%M:%S"
    ) + timedelta(hours=4)
    formatted_datetime = converted_datetime.strftime("%Y-%m-%d %H:%M:%S")

    return formatted_datetime


def upload_checkpoints_voz_to_stage(df: pd.DataFrame, table: str) -> None:
    """
    Função responsável por realizar o upload dos dados extraídos do Five9 na tabela de checkpoints de mídia voz no banco STAGE.

    Args:
        df (pd.DataFrame): Nome do report em formato DataFrame extraído do Five9 via API.
        table (str): Nome da tabela que será salvo os dados no banco STAGE.

    Returns:
        None
    """
    engine, session = get_connection_from_db("STAGE")

    # Aplica a função à coluna e cria um nova coluna "data_inicio_checkpoints"
    df["data_inicio_checkpoints"] = df["MODULE START TIMESTAMP"].apply(
        convert_datetime_with_timezone
    )

    # Dropar, renomear e ordenar as colunas
    df = df.drop(columns=["DATE", "TIME", "MODULE START TIMESTAMP"])
    df = df.rename(
        columns={
            "CALL ID": "id_chamada",
            "data_ligacao": "data_inicio_chamada",
            "CAMPAIGN": "campanha",
            "MODULE": "checkpoints",
        }
    )
    df = df[
        [
            "data_extracao",
            "id_chamada",
            "data_inicio_chamada",
            "campanha",
            "data_inicio_checkpoints",
            "checkpoints",
        ]
    ]

    logger.info(
        f"Total de registros a serem inseridos na tabela five9.stg_{table} do banco de dados STAGE: {df['id_chamada'].count()}"
    )
    # Carrega os dados para a tabela five9.stg_checkpoints_voz do banco de dados STAGE.
    load_data(
        data=df,
        table_name="stg_" + table,
        schema="five9",
        database="STAGE",
        engine=engine,
        session=session,
        if_exists="replace",
    )


def upload_to_stage_voz(lista_df: List[pd.DataFrame], tables_stg: List[str]) -> None:
    """
    Função responsável por realizar o upload dos dados extraídos do Five9 nas tabelas de recebidas de mídia de voz no banco STAGE.

    Args:
        lista_df (List[pd.DataFrame]): Lista dos reports em formato dataframe extraídos do Five9 via API.
        tables_stg (List[str]): Lista de tabelas do banco STAGE.

    Returns:
        None
    """
    engine, session = get_connection_from_db("STAGE")

    for i in range(len(tables_stg)):
        if "transferidas" not in tables_stg[i]:
            # Inserindo dados na tabela pré-stage
            load_data(
                data=lista_df[i],
                table_name=tables_stg[i],
                schema="five9",
                database="STAGE",
                engine=engine,
                session=session,
                if_exists="replace",
            )
        try:
            with engine.connect() as conn, conn.begin():
                conn.execute(text(f"EXEC STAGE.five9.proc_stg_{tables_stg[i]}"))
                conn.commit()
                logger.info(
                    f"Procedure five9.proc_stg_{tables_stg[i]} do banco de dados STAGE foi executada com sucesso."
                )
        except Exception as e:
            logger.error(
                f"Falha na execução da procedure five9.proc_stg_{tables_stg[i]} do banco de dados STAGE. Motivo do erro: {e}"
            )
            exit(1)


def upload_to_stage_chat(lista_df: List[pd.DataFrame], tables_stg: List[str]) -> None:
    """
    Função responsável por realizar o upload dos dados extraídos da Five9 nas tabelas de recebidas de mídia de chat no banco STAGE.

    Args:
        lista_df (List[pd.DataFrame]): Lista dos reports em formato dataframe extraídos do Five9 via API.
        tables_stg (List[str]): Lista de tabelas do banco STAGE.

    Returns:
        None
    """
    engine, session = get_connection_from_db("STAGE")

    for i in range(len(tables_stg)):
        load_data(
            data=lista_df[i],
            table_name=tables_stg[i],
            schema="five9",
            database="STAGE",
            engine=engine,
            session=session,
            if_exists="replace",
        )
    try:
        with engine.connect() as conn, conn.begin():
            conn.execute(text(f"EXEC STAGE.five9.proc_stg_{tables_stg[0]}"))
            conn.commit()
            logger.info(
                f"Procedure five9.proc_stg_{tables_stg[0]} do banco de dados STAGE foi executada com sucesso."
            )
    except Exception as e:
        logger.error(
            f"Falha na execução da procedure five9.proc_stg_{tables_stg[0]} do banco de dados STAGE. Motivo do erro: {e}"
        )
        exit(1)


async def teste():
    """Função responsável por testar o módulo."""
    folder_name = "Meus relatorios"
    reports_name = [
        "Report Voz - Ligacoes Receptivas com Retencao - v2",
        "Report Voz - TMA",
        "Report Voz - Disconnect First",
        "Report Voz - Ligacoes Ativas",
    ]
    tables_stg = [
        "chamadas_recebidas_voz",
        "tma",
        "disconnect_first_voz",
        "chamadas_ativas_voz",
        "tma_transferidas",
    ]
    startAt = "2023-10-03T09:00:00.000-03:00"
    endAt = "2023-10-03T16:59:59.000-03:00"
    hours = 39

    lista_df = await upload_to_dbapi_voz(
        folder_name, reports_name, tables_stg, startAt, endAt, hours
    )

    upload_to_stage_voz(lista_df, tables_stg)


if __name__ == "__main__":
    import asyncio

    import nest_asyncio

    nest_asyncio.apply()

    filepath = "c:/etl/five9/logs/Five9_Voz_ChamadasAtivas.log"

    logger.remove()
    logger.add(
        sink=filepath,
        format="{time:MMMM D, YYYY > HH:mm:ss} | {level} | {name} | {message}",
        level="INFO",
        rotation="5 MB",
    )

    asyncio.run(teste())
