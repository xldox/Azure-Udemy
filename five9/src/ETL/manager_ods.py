"""Esse módulo é o responsável pelos inputs no banco de dados ODS."""

from datetime import datetime, timedelta
from typing import List

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import text

from .manager_db import get_connection_from_db, load_data
from .manager_dbapi import upload_to_dbapi_chat, upload_to_dbapi_voz
from .manager_dw import upload_to_dw_chat, upload_to_dw_voz
from .manager_stage import upload_to_stage_chat, upload_to_stage_voz


def upload_to_ods_voz(tables_ft: List[str]) -> None:
    """
    Função responsável por realizar o upload dos dados extraídos do Five9 nas tabelas de Chat no banco ODS.

    Args:
        tables_ft (List[str]): Lista de nomes para as tabelas do banco de dados ODS.

    Returns:
        None
    """
    engine_dw, session_dw = get_connection_from_db("DBDW")
    engine_ods, session_ods = get_connection_from_db("ODS")

    for table in tables_ft:
        query_ods = f"""
            SELECT
                id_chamada

            FROM ODS.five9.fato_{table}

            WHERE data_inicio_chamada >= :data_inicio_chamada
        """
        try:
            with engine_ods.connect() as conn, conn.begin():
                df_from_ods = pd.read_sql_query(
                    sql=text(query_ods),
                    con=conn,
                    params={
                        "data_inicio_chamada": (
                            datetime.now() - timedelta(hours=9)
                        ).strftime("%Y-%m-%d %H:%M:%S")
                    },
                )
        except Exception as e:
            logger.error(
                f"Falha na leitura da query da tabela five9.fato_{table} do banco de dados ODS. Motivo do erro: {e}"
            )
            exit(1)

        query_dw = f"""
            SELECT
                *

            FROM DBDW.five9.fato_{table}

            WHERE data_inicio_chamada >= :data_inicio_chamada
        """
        try:
            with engine_dw.connect() as conn, conn.begin():
                df_from_dw = pd.read_sql_query(
                    sql=text(query_dw),
                    con=conn,
                    params={
                        "data_inicio_chamada": (
                            datetime.now() - timedelta(hours=7)
                        ).strftime("%Y-%m-%d %H:%M:%S")
                    },
                )
        except Exception as e:
            logger.error(
                f"Falha na leitura da query da tabela five9.fato_{table} do banco de dados DBDW. Motivo do erro: {e}"
            )
            exit(1)

        df_filtered = (
            df_from_dw.merge(df_from_ods, on=["id_chamada"], how="left", indicator=True)
            .query("_merge == 'left_only'")
            .drop(columns=["_merge"])
        )

        logger.info(
            f"Total de registros a serem inseridos na tabela five9.fato_{table} do banco de dados ODS: {df_filtered['id_chamada'].count()}"
        )
        load_data(
            data=df_filtered.drop(["id"], axis=1),
            table_name="fato_" + table,
            schema="five9",
            database="ODS",
            engine=engine_ods,
            session=session_ods,
            if_exists="append",
        )


def upload_to_ods_chat(tables_ft: List[str]) -> None:
    """
    Função responsável por realizar o upload dos dados extraídos do Five9 nas tabelas de Chat no banco ODS.

    Args:
        tables_ft (List[str]): Lista de nomes para as tabelas do banco de dados ODS.
        hours (int): Quantidade de horas passadas que deverá ser buscado a informação nas queries.

    Returns:
        None
    """
    engine_dw, session_dw = get_connection_from_db("DBDW")
    engine_ods, session_ods = get_connection_from_db("ODS")

    for table in tables_ft:
        query_ods = f"""
            SELECT
                id_chamada

            FROM ODS.five9.fato_{table}

            WHERE data_inicio_chamada >= :data_inicio_chamada
        """
        try:
            with engine_ods.connect() as conn, conn.begin():
                df_from_ods = pd.read_sql_query(
                    sql=text(query_ods),
                    con=conn,
                    params={
                        "data_inicio_chamada": (
                            datetime.now() - timedelta(hours=9)
                        ).strftime("%Y-%m-%d %H:%M:%S")
                    },
                )
        except Exception as e:
            logger.error(
                f"Falha na leitura da query da tabela five9.fato_{table} do banco de dados ODS. Motivo do erro: {e}"
            )
            exit(1)

        query_dw = f"""
            SELECT
                *

            FROM DBDW.five9.fato_{table}

            WHERE data_inicio_chamada >= :data_inicio_chamada
        """
        try:
            with engine_dw.connect() as conn, conn.begin():
                df_from_dw = pd.read_sql_query(
                    sql=text(query_dw),
                    con=conn,
                    params={
                        "data_inicio_chamada": (
                            datetime.now() - timedelta(hours=7)
                        ).strftime("%Y-%m-%d %H:%M:%S")
                    },
                )
        except Exception as e:
            logger.error(
                f"Falha na leitura da query da tabela five9.fato_{table} do banco de dados DBDW. Motivo do erro: {e}"
            )
            exit(1)

        df_filtered = (
            df_from_dw.merge(df_from_ods, on=["id_chamada"], how="left", indicator=True)
            .query("_merge == 'left_only'")
            .drop(columns=["_merge"])
        )

        logger.info(
            f"Total de registros a serem inseridos na tabela five9.fato_{table} do banco de dados ODS: {df_filtered['id_chamada'].count()}"
        )
        load_data(
            data=df_filtered.drop(["id"], axis=1),
            table_name="fato_" + table,
            schema="five9",
            database="ODS",
            engine=engine_ods,
            session=session_ods,
            if_exists="append",
        )


async def teste():
    """Função responsável por testar o módulo."""
    # folder_name = "Meus relatorios"
    # reports_name = [
    #     "Report Voz - Ligacoes Receptivas com Retencao - v2",
    #     "Report Voz - TMA",
    #     "Report Voz - Disconnect First",
    #     "Report Voz - Ligacoes Ativas",
    # ]
    # tables_stg = [
    #     "chamadas_recebidas_voz",
    #     "tma",
    #     "disconnect_first_voz",
    #     "chamadas_ativas_voz",
    # ]
    tables_ft = ["chamadas_recebidas_voz", "chamadas_ativas_voz"]
    # startAt = "2023-10-03T00:00:00.000-03:00"
    # endAt = "2023-10-03T08:59:59.000-03:00"
    # hours = 10

    # lista_df = await upload_to_dbapi_voz(
    #     folder_name, reports_name, tables_stg, startAt, endAt, hours
    # )

    # upload_to_stage_voz(lista_df, tables_stg)

    upload_to_dw_voz(tables_ft)

    upload_to_ods_voz(tables_ft)


if __name__ == "__main__":
    import asyncio

    import nest_asyncio

    nest_asyncio.apply()

    asyncio.run(teste())
