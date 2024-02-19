"""Esse módulo é o responsável pelos inputs no banco de dados DBAPI."""

from datetime import datetime, timedelta
from io import StringIO
from typing import List

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import text

from .added_date_columns import add_quality_columns
from .manager_db import get_connection_from_db, load_data
from .manager_five9 import get_report_result_as_csv


async def upload_checkpoints_voz_to_dbapi(
    folder_name: str,
    report_name: str,
    table: str,
    startAt: str,
    endAt: str,
) -> pd.DataFrame:
    """
    Função responsável por realizar o upload dos dados extraídos do report de Checkpoints de Voz do Five9 na tabela do banco DBAPI.

    Args:
        folder_name (str): Nome da pasta localizada na plataforma do Five9.
        report_name (str): Nome do report salvo na plataforma do Five9.
        table (str): Nome da tabela do banco de dados DBAPI.
        startAt (str): Data inicial desejada de extração.
        endAt (str): Data final desejada de extração.

    Returns:
        pd.DataFrame: Dataframe do report extraído do Five9.
    """
    engine, session = get_connection_from_db("DBAPI")

    try:
        df = pd.read_csv(
            StringIO(
                await get_report_result_as_csv(folder_name, report_name, startAt, endAt)
            ),
            sep=",",
            dtype="object",
        )
        logger.info(
            f"Quantidade de dados baixados do {report_name}: {df['CALL ID'].count()}"
        )
    except Exception as e:
        logger.error(
            f"Falha ao tentar transformar em dataframe o {report_name}. Motivo do erro: {e}"
        )

    try:
        if df.empty:
            df["data_ligacao"], df["data_extracao"] = np.nan
        else:
            df = add_quality_columns(df)
    except Exception as e:
        logger.error(
            f"Falha ao tentar transformar colunas no dataframe. Motivo do erro: {e}"
        )

    # Eliminando linhas nulas da coluna "CALL ID" transformando as colunas em string
    df.dropna(subset=["CALL ID"], inplace=True)
    df_remove = df.loc[df["CALL ID"] == "nan"]
    df = df.drop(df_remove.index)
    df = df.astype(str)

    query = f"""
        SELECT
            [CALL ID]
            , [MODULE START TIMESTAMP]
            , [MODULE]

        FROM DBAPI.five9.{table}

        WHERE data_ligacao >= :data_ligacao
    """
    try:
        with engine.connect() as conn, conn.begin():
            df_from_dbapi = pd.read_sql_query(
                sql=text(query),
                con=conn,
                params={
                    "data_ligacao": (datetime.now() - timedelta(hours=7)).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                },
            )
    except Exception as e:
        logger.error(
            f"Falha na leitura da query da tabela five9.{table} do banco de dados DBAPI. Motivo do erro: {e}"
        )
        exit(1)

    df_filtered = (
        df.merge(
            df_from_dbapi,
            on=["CALL ID", "MODULE START TIMESTAMP", "MODULE"],
            how="left",
            indicator=True,
        )
        .query("_merge == 'left_only'")
        .drop(columns=["_merge"])
    )

    logger.info(
        f"Total de registros a serem inseridos na tabela five9.{table} do banco de dados DBAPI: {df_filtered['CALL ID'].count()}"
    )
    # Carrega os dados para a tabela five9.checkpoints_voz do banco de dados DBAPI.
    load_data(
        data=df_filtered,
        table_name=table,
        schema="five9",
        database="DBAPI",
        engine=engine,
        session=session,
        if_exists="append",
    )

    return df


async def upload_to_dbapi_voz(
    folder_name: str,
    reports_name: List[str],
    tables: List[str],
    startAt: str,
    endAt: str,
) -> List[pd.DataFrame]:
    """
    Função responsável por realizar o upload dos dados extraídos do Five9 nas tabelas de voz no banco DBAPI.

    Args:
        folder_name (str): Nome da pasta localizada na plataforma do Five9.
        reports_name (List[str]): Uma lista de nomes de reports.
        tables (List[str]): Uma lista de nomes das tabelas do banco de dados DBAPI.
        startAt (str): Data inicial desejada de extração.
        endAt (str): Data final desejada de extração.

    Returns:
        List[pd.DataFrame]: Lista com dataframes extraídos do Five9.
    """
    engine, session = get_connection_from_db("DBAPI")

    lista_df_api = []
    for i in range(len(reports_name)):
        try:
            df = pd.read_csv(
                StringIO(
                    await get_report_result_as_csv(
                        folder_name, reports_name[i], startAt, endAt
                    )
                ),
                sep=",",
                dtype="object",
            )
            logger.info(
                f"Quantidade de dados baixados do {reports_name[i]}: {df['CALL ID'].count()}"
            )
        except Exception as e:
            logger.error(
                f"Falha ao tentar transformar em dataframe o {reports_name[i]}. Motivo do erro: {e}"
            )
            exit(1)

        try:
            if df.empty:
                df["data_ligacao"], df["data_extracao"] = np.nan
            else:
                df = add_quality_columns(df)
        except Exception as e:
            logger.error(
                f"Falha ao tentar transformar colunas no dataframe. Motivo do erro: {e}"
            )

        # Substituindo caracter '.0' da coluna 'CALL ID' da tabela de discoonectFirst
        if "disconnect" in tables[i]:
            df["CALL ID"] = df["CALL ID"].replace(".0", "", regex=True)

        # Eliminando linhas nulas da coluna "CALL ID" transformando as colunas em string
        df.dropna(subset=["CALL ID"], inplace=True)
        df_remove = df.loc[df["CALL ID"] == "nan"]
        df = df.drop(df_remove.index)
        df = df.astype(str)

        query = f"""
            SELECT
                [CALL ID]

            FROM DBAPI.five9.{tables[i]}_v2

            WHERE data_ligacao >= :data_ligacao
        """
        try:
            with engine.connect() as conn, conn.begin():
                df_from_sql = pd.read_sql_query(
                    sql=text(query),
                    con=conn,
                    params={
                        "data_ligacao": (datetime.now() - timedelta(hours=7)).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    },
                )
        except Exception as e:
            logger.error(
                f"Falha na leitura da query da tabela five9.{tables[i]} do banco de dados DBAPI. Motivo do erro: {e}"
            )
            exit(1)

        df_filtered = (
            df.merge(df_from_sql, on=["CALL ID"], how="left", indicator=True)
            .query("_merge == 'left_only'")
            .drop(columns=["_merge"])
        )

        logger.info(
            f"Total de registros a serem inseridos na tabela five9.{tables[i]} do banco de dados DBAPI: {df_filtered['data_ligacao'].count()}"
        )
        load_data(
            data=df_filtered,
            table_name=tables[i] + "_v2",
            schema="five9",
            database="DBAPI",
            engine=engine,
            session=session,
            if_exists="append",
        )

        lista_df_api.append(df)

    return lista_df_api


async def upload_to_dbapi_chat(
    folder_name: str,
    reports_name: List[str],
    tables: List[str],
    startAt: str,
    endAt: str,
) -> List[pd.DataFrame]:
    """
    Função responsável por realizar o upload dos dados extraídos do Five9 nas tabelas de chat no banco DBAPI.

    Args:
        folder_name (str): Nome da pasta localizada na plataforma do Five9.
        reports_name (List[str]): Uma lista de nomes de reports.
        tables (List[str]): Uma lista de nomes das tabelas do banco de dados DBAPI.
        startAt (str): Data inicial desejada de extração.
        endAt (str): Data final desejada de extração.
        hours (int): Quantidade de horas passadas que deverá ser buscado a informação.

    Returns:
        List[pd.DataFrame]: Lista com dataframes extraídos do Five9.
    """
    engine, session = get_connection_from_db("DBAPI")

    lista_df_api = []
    for i in range(len(reports_name)):
        try:
            df = pd.read_csv(
                StringIO(
                    await get_report_result_as_csv(
                        folder_name, reports_name[i], startAt, endAt
                    )
                ),
                sep=",",
                dtype="object",
            )
            logger.info(
                f"Quantidade de dados baixados do {reports_name[i]}: {df['SESSION GUID'].count() if 'base' not in tables[i] else df['CALL ID'].count()}"
            )
        except Exception as e:
            logger.error(
                f"Falha ao tentar transformar em dataframe o {reports_name[i]}. Motivo do erro: {e}"
            )
            exit(1)

        try:
            if df.empty:
                df["data_ligacao"], df["data_extracao"] = np.nan
            else:
                df = add_quality_columns(df)
        except Exception as e:
            logger.error(
                f"Falha ao tentar transformar colunas no dataframe. Motivo do erro: {e}"
            )

        if "base" in tables[i]:
            query = f"""
                SELECT
                    [CALL ID]

                FROM DBAPI.five9.{tables[i]}

                WHERE data_ligacao >= :data_ligacao
            """
        else:
            query = f"""
                SELECT
                    [SESSION GUID]

                FROM DBAPI.five9.{tables[i]}

                WHERE data_ligacao >= :data_ligacao
            """
        try:
            with engine.connect() as conn, conn.begin():
                df_from_sql = pd.read_sql_query(
                    sql=text(query),
                    con=conn,
                    params={
                        "data_ligacao": (datetime.now() - timedelta(hours=7)).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    },
                )
        except Exception as e:
            logger.error(
                f"Falha na leitura da query da tabela five9.{tables[i]} do banco de dados DBAPI. Motivo do erro: {e}"
            )
            exit(1)

        # Eliminando linhas nulas da coluna "CALL ID" transformando as colunas em string
        df.dropna(subset=["CALL ID"], inplace=True) if "base" in tables[
            i
        ] else df.dropna(subset=["SESSION GUID"], inplace=True)
        df = df.astype(str)

        df_filtered = (
            df.merge(df_from_sql, on=["SESSION GUID"], how="left", indicator=True)
            .query("_merge == 'left_only'")
            .drop(columns=["_merge"])
            if "base" not in tables[i]
            else df.merge(df_from_sql, on=["CALL ID"], how="left", indicator=True)
            .query("_merge == 'left_only'")
            .drop(columns=["_merge"])
        )
        logger.info(
            f"Total de registros a serem inseridos na tabela five9.{tables[i]} do banco de dados DBAPI: {df_filtered['data_ligacao'].count()}"
        )

        load_data(
            data=df_filtered,
            table_name=tables[i],
            schema="five9",
            database="DBAPI",
            engine=engine,
            session=session,
            if_exists="append",
        )

        lista_df_api.append(df)

    return lista_df_api


async def teste():
    """Função responsável por testar o módulo."""
    folder_name = "Meus relatorios"
    reports_name = [
        "Report Voz - Disconnect First",
        "Report Voz - Ligacoes Receptivas com Retencao - v2",
        "Report Voz - TMA",
        "Report Voz - Ligacoes Ativas",
    ]
    tables_stg = [
        "disconnect_first_voz",
        "chamadas_recebidas_voz",
        "tma",
        "chamadas_ativas_voz",
    ]
    startAt = "2023-10-03T08:59:59.000-03:00"
    endAt = "2023-10-03T23:59:59.000-03:00"
    hours = 58

    await upload_to_dbapi_voz(
        folder_name, reports_name, tables_stg, startAt, endAt, hours
    )


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
