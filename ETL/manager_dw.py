"""Esse módulo é o responsável pelos inputs no banco de dados DBDW."""

from datetime import datetime, timedelta
from typing import List

import pandas as pd
from loguru import logger
from sqlalchemy import text

from .manager_db import get_connection_from_db, load_data
from .manager_dbapi import upload_to_dbapi_chat, upload_to_dbapi_voz
from .manager_stage import upload_to_stage_chat, upload_to_stage_voz


def add_quality_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona colunas de qualidade ao DataFrame fornecido.

    Args:
        df (pd.DataFrame): DataFrame em que será adicionado as colunas de qualidade.
        etl_type (str): Tipo do ETL que está sendo tratado. Padrão: 'Checkpoints'.

    Returns:
        pd.DataFrame: Retorna o DataFrame com colunas de qualidade adicionadas.
    """
    # Adiciona a coluna que registra quando que foi inserido os dados na tabela
    df["data_input"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return df


def upload_checkpoints_voz_to_dw(table: str) -> None:
    """
    Função responsável por realizar o upload dos dados extraídos do Five9 na tabela de checkpoints de mídia voz no banco DBDW.

    Args:
        table (str): Nome da tabela em que será salvo os dados no banco de dados DBDW.

    Returns:
        None
    """
    engine_dw, session_dw = get_connection_from_db("DBDW")
    engine_stg, session_stg = get_connection_from_db("STAGE")

    query = f"""
        SELECT
            id_chamada
            , data_inicio_checkpoints
            , checkpoints

        FROM DBDW.five9.fato_{table}

        WHERE data_inicio_chamada >= :data_inicio_chamada
    """
    try:
        with engine_dw.connect() as conn, conn.begin():
            df_from_ft = pd.read_sql_query(
                sql=text(query),
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

    query = f"""
        SELECT *

        FROM STAGE.five9.stg_{table}
    """
    try:
        with engine_stg.connect() as conn, conn.begin():
            df_from_stg = pd.read_sql_query(
                sql=text(query),
                con=conn,
            )
    except Exception as e:
        logger.error(
            f"Falha na leitura da query da tabela five9.fato_{table} do banco de dados DBDW. Motivo do erro: {e}"
        )
        exit(1)

    # Garantindo que todos os tipos das colunas sejam do tipo 'object'
    df_from_ft = df_from_ft.astype(str)
    df_from_stg = df_from_stg.astype(str)

    df_filtered = (
        df_from_stg.merge(
            df_from_ft,
            on=["id_chamada", "data_inicio_checkpoints", "checkpoints"],
            how="left",
            indicator=True,
        )
        .query("_merge == 'left_only'")
        .drop(columns=["_merge"])
    )

    # Adiciona colunas de qualidade
    df = add_quality_columns(df_filtered)

    # Ordena o DataFrame final
    df = df[
        [
            "data_input",
            "data_extracao",
            "id_chamada",
            "data_inicio_chamada",
            "campanha",
            "data_inicio_checkpoints",
            "checkpoints",
        ]
    ]

    logger.info(
        f"Total de registros a serem inseridos na tabela five9.fato_{table} do banco de dados DBDW: {df['id_chamada'].count()}"
    )
    # Carrega os dados exclusivos para a tabela "five9.fato_checkpoints_voz" do DW
    load_data(
        data=df,
        table_name="fato_" + table,
        schema="five9",
        database="DBDW",
        engine=engine_dw,
        session=session_dw,
        if_exists="append",
    )


def upload_to_dw_voz(tables_ft: List[str]) -> None:
    """
    Função responsável por realizar o upload dos dados extraídos do Five9 nas tabelas de Voz no banco DBDW.

    Args:
        tables_ft (List[str]): Lista de nomes para as tabelas do banco de dados DBDW.

    Returns:
        None
    """
    engine_dw, session_dw = get_connection_from_db("DBDW")
    engine_stg, session_stg = get_connection_from_db("STAGE")

    for table in tables_ft:
        query = f"""
            SELECT
                id_chamada

            FROM DBDW.five9.fato_{table}

            WHERE data_inicio_chamada >= :data_inicio_chamada
        """
        try:
            with engine_dw.connect() as conn, conn.begin():
                df_from_dw = pd.read_sql_query(
                    sql=text(query),
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

        if "recebidas" in table:
            query = """
                WITH cte_transferidas AS
                (
                    SELECT
                        V.id_chamada
                        , T.data_inicio_chamada
                        , T.data_inicio_atendimento
                        , V.campanha
                        , T.servico
                        , T.destino_transferencia
                        , T.segmento AS agente
                        , T.tipo_segmento
                        , CASE
                            WHEN T.indice = T2.indice_max THEN V.resultado
                            ELSE NULL
                        END AS resultado
                        , V.servico AS servico_final
                        , CASE
                            WHEN T.indice = T2.indice_max THEN V.classificacao_grupo_a
                            ELSE NULL
                        END AS classificacao_grupo_a
                        , CASE
                            WHEN T.indice = T2.indice_max THEN V.classificacao_grupo_b
                            ELSE NULL
                        END AS classificacao_grupo_b
                        , CASE
                            WHEN T.indice = T2.indice_max THEN V.classificacao_grupo_c
                            ELSE NULL
                        END AS classificacao_grupo_c
                        , V.motivo_retencao_ura
                        , V.tipo_chamada
                        , V.cpf_cnpj
                        , V.contrato
                        , V.origem
                        , V.destino
                        , CASE
                            WHEN T.indice = T2.indice_max THEN 0
                            ELSE V.transferida
                        END AS transferida
                        , V.abandonada
                        , V.massiva
                        , V.cidade
                        , V.bairro
                        , V.endereco
                        , CASE
                            WHEN T.indice = T2.indice_max THEN V.pergunta_satisfacao_1
                            ELSE NULL
                        END AS pergunta_satisfacao_1
                        , CASE
                            WHEN T.indice = T2.indice_max THEN V.pergunta_satisfacao_2
                            ELSE NULL
                        END AS pergunta_satisfacao_2
                        , CASE
                            WHEN T.indice = T2.indice_max THEN V.pergunta_satisfacao_3
                            ELSE NULL
                        END AS pergunta_satisfacao_3
                        , T.tempo_chamada
                        , T.tempo_conversa
                        , T.tempo_espera_fila
                        , T.tempo_toque
                        , T.tempo_pos_atendimento
                        , CASE
                            WHEN indice = indice_max
                                THEN IIF(dfv.agente_desconecta_primeiro = '1', 'Destino', IIF(dfv.agente_desconecta_primeiro = '0', 'Origem', NULL))
                            ELSE NULL
                        END AS desligado_por
                        , V.marca
                        , V.polo
                        , V.data_extracao
                        , V.tempo_ivr

                    FROM STAGE.five9.stg_chamadas_recebidas_voz_v2 V
                    INNER JOIN STAGE.five9.stg_tma_transferidas_v2 T ON T.id_chamada = V.id_chamada
                    INNER JOIN (SELECT MAX(indice) AS indice_max, id_chamada
                                FROM STAGE.five9.stg_tma_transferidas_v2
                                GROUP BY id_chamada) T2 ON T2.id_chamada = T.id_chamada
                    LEFT JOIN STAGE.five9.stg_disconnect_first_voz_v2 dfv ON V.id_chamada = dfv.id_chamada AND
                                                                            T.data_inicio_chamada = dfv.data_inicio_chamada AND
                                                                            V.agente = dfv.email_agente
                ),
                cte_nao_transferidas AS
                (
                    SELECT
                        V.id_chamada
                        , V.data_inicio_chamada
                        , null AS data_inicio_atendimento
                        , V.campanha
                        , V.servico
                        , null AS destino_transferencia
                        , V.agente
                        , null AS tipo_segmento
                        , V.resultado
                        , V.servico AS servico_final
                        , V.classificacao_grupo_a
                        , V.classificacao_grupo_b
                        , V.classificacao_grupo_c
                        , V.motivo_retencao_ura
                        , V.tipo_chamada
                        , V.cpf_cnpj
                        , V.contrato
                        , V.origem
                        , V.destino
                        , V.transferida
                        , V.abandonada
                        , V.massiva
                        , V.cidade
                        , V.bairro
                        , V.endereco
                        , V.pergunta_satisfacao_1
                        , V.pergunta_satisfacao_2
                        , V.pergunta_satisfacao_3
                        , T.tempo_chamada
                        , T.tempo_conversa
                        , T.tempo_espera_fila
                        , T.tempo_toque
                        , T.tempo_pos_atendimento
                        , IIF(dfv.agente_desconecta_primeiro = '1', 'Destino', IIF(dfv.agente_desconecta_primeiro = '0', 'Origem', NULL)) AS desligado_por
                        , V.marca
                        , V.polo
                        , V.data_extracao
                        , V.tempo_ivr

                    FROM STAGE.five9.stg_chamadas_recebidas_voz_v2 V
                    LEFT JOIN STAGE.five9.stg_tma_v2 T ON T.id_chamada = V.id_chamada
                    LEFT JOIN STAGE.five9.stg_disconnect_first_voz_v2 dfv ON V.id_chamada = dfv.id_chamada AND
                                                                            V.agente = dfv.email_agente

                    WHERE V.id_chamada NOT IN (SELECT DISTINCT id_chamada FROM STAGE.five9.stg_tma_transferidas_v2)
                ),
                cte_unifica_bases AS
                (
                    SELECT
                        data_extracao, id_chamada, data_inicio_chamada, data_inicio_atendimento
                        , campanha, agente, servico, tipo_segmento, destino_transferencia, resultado
                        , servico_final, classificacao_grupo_a, classificacao_grupo_b, classificacao_grupo_c
                        , motivo_retencao_ura, tipo_chamada, cpf_cnpj, contrato, origem, destino
                        , transferida, abandonada, massiva, cidade, bairro, endereco
                        , pergunta_satisfacao_1, pergunta_satisfacao_2, pergunta_satisfacao_3
                        , tempo_chamada, tempo_conversa, tempo_espera_fila, tempo_toque, tempo_pos_atendimento
                        , desligado_por, marca, polo, tempo_ivr

                    FROM cte_transferidas

                    UNION

                    SELECT
                        data_extracao, id_chamada, data_inicio_chamada, data_inicio_atendimento
                        , campanha, agente, servico, tipo_segmento, destino_transferencia, resultado
                        , servico_final, classificacao_grupo_a, classificacao_grupo_b, classificacao_grupo_c
                        , motivo_retencao_ura, tipo_chamada, cpf_cnpj, contrato, origem, destino
                        , transferida, abandonada, massiva, cidade, bairro, endereco
                        , pergunta_satisfacao_1, pergunta_satisfacao_2, pergunta_satisfacao_3
                        , tempo_chamada, tempo_conversa, MAX(tempo_espera_fila) AS tempo_espera_fila, tempo_toque, tempo_pos_atendimento
                        , desligado_por, marca, polo, tempo_ivr

                    FROM cte_nao_transferidas

                    GROUP BY id_chamada, data_inicio_chamada, data_inicio_atendimento
                        , campanha, agente, servico, tipo_segmento, destino_transferencia, resultado
                        , servico_final, classificacao_grupo_a, classificacao_grupo_b, classificacao_grupo_c
                        , motivo_retencao_ura, tipo_chamada, cpf_cnpj, contrato, origem, destino
                        , transferida, abandonada, massiva, cidade, bairro, endereco
                        , pergunta_satisfacao_1, pergunta_satisfacao_2, pergunta_satisfacao_3
                        , tempo_chamada, tempo_conversa, tempo_toque, tempo_pos_atendimento
                        , desligado_por, marca, polo, data_extracao, tempo_ivr
                ),
                cte_base_clientes_alloha AS
                (
                SELECT
                    contrato
                    , REPLACE(REPLACE(cidade, CHAR(13), ''), CHAR(10), '') AS cidade
                    , bairro
                    , CASE
                        WHEN marca = 'GIGA+' THEN 'GIGA+ FIBRA'
                        WHEN marca = 'NIU FIBRA' THEN 'NIU'
                        WHEN marca = 'MOB' THEN 'MOBWIRE'
                        ELSE marca
                    END AS marca
                    , REPLACE(polo, 'POLO ', '') AS polo

                FROM DBDW.dbo.base_clientes_alloha

                WHERE CAST(data_referencia AS DATE) >= CAST(DATEADD(HOUR, -3, GETDATE() - 2) AS DATE)
                )
                SELECT DISTINCT
                    CONVERT(VARCHAR, DATEADD(HOUR, -3, GETDATE()), 120) AS data_input
                    , data_extracao
                    , id_chamada
                    , data_inicio_chamada
                    , data_inicio_atendimento
                    , campanha
                    , agente
                    , servico
                    , tipo_segmento
                    , destino_transferencia
                    , resultado
                    , servico_final
                    , classificacao_grupo_a, classificacao_grupo_b, classificacao_grupo_c
                    , motivo_retencao_ura
                    , tipo_chamada
                    , cpf_cnpj, ch.contrato, origem, destino, transferida, abandonada, massiva
                    , IIF(ch.cidade IS NULL, IIF(cli.cidade IS NULL, cli_vip.cidade, cli.cidade), ch.cidade) AS cidade
                    , IIF(ch.bairro IS NULL, IIF(cli.bairro IS NULL, cli_vip.bairro, cli.bairro), ch.bairro) AS bairro
                    , endereco
                    , pergunta_satisfacao_1, pergunta_satisfacao_2, pergunta_satisfacao_3
                    , tempo_chamada, tempo_conversa, tempo_espera_fila, tempo_toque, tempo_pos_atendimento
                    , desligado_por
                    , ch.marca, ch.polo, tempo_ivr

                FROM cte_unifica_bases ch
                LEFT JOIN (SELECT contrato, polo, cidade, bairro FROM cte_base_clientes_alloha WHERE polo <> 'VIP') cli ON ch.contrato = cli.contrato AND
                                                                                                                        ch.polo = cli.polo
                LEFT JOIN (SELECT contrato, marca, cidade, bairro FROM cte_base_clientes_alloha WHERE polo = 'VIP') cli_vip ON ch.contrato = cli_vip.contrato AND
                                                                                                                            ch.marca = cli_vip.marca
            """
        else:
            query = """
                SELECT DISTINCT
                    CONVERT(VARCHAR, DATEADD(HOUR, -3, GETDATE()), 120) AS data_input
                    , V.data_extracao
                    , V.id_chamada
                    , V.data_inicio_chamada
                    , V.campanha
                    , V.agente
                    , V.resultado
                    , V.tipo_chamada
                    , V.cliente
                    , V.cpf_cnpj
                    , V.contrato
                    , V.destino
                    , V.transferida
                    , V.abandonada
                    , V.tempo_conversa
                    , IIF(dfv.agente_desconecta_primeiro = '1', 'Origem', IIF(dfv.agente_desconecta_primeiro = '0', 'Destino', NULL)) AS desligado_por
                    , V.id_fatura
                    , V.marca
                    , V.polo
                    , V.classificacao_grupo_a
                    , V.classificacao_grupo_b
                    , V.classificacao_grupo_c

                FROM STAGE.five9.stg_chamadas_ativas_voz_v2 V
                LEFT JOIN STAGE.five9.stg_disconnect_first_voz_v2 dfv ON V.id_chamada = dfv.id_chamada AND
                                                                        V.agente = dfv.email_agente
            """
        try:
            with engine_stg.connect() as conn, conn.begin():
                df_from_stg = pd.read_sql_query(sql=text(query), con=conn)
        except Exception as e:
            if "recebidas" in table:
                logger.error(
                    f"Falha na leitura da query vindo da STAGE que irá inserir dados na tabela five9.fato_{table} do banco de dados DBDW. Motivo do erro: {e}"
                )
            else:
                logger.error(
                    f"Falha na leitura da query vindo da STAGE que irá inserir dados na tabela five9.fato_{table} do banco de dados DBDW. Motivo do erro: {e}"
                )
            exit(1)

        df_filtered = (
            df_from_stg.merge(df_from_dw, on=["id_chamada"], how="left", indicator=True)
            .query("_merge == 'left_only'")
            .drop(columns=["_merge"])
        )

        logger.info(
            f"Total de registros a serem inseridos na tabela five9.fato_{table} do banco de dados DBDW: {df_filtered['id_chamada'].count()}"
        )
        # Carrega os dados filtrados para as tabelas 'fato_chamadas_recebidas_voz' e 'fato_chamadas_ativas_voz' do DW
        load_data(
            data=df_filtered,
            table_name="fato_" + table,
            schema="five9",
            database="DBDW",
            engine=engine_dw,
            session=session_dw,
            if_exists="append",
        )


def upload_to_dw_chat(tables_ft: List[str]) -> None:
    """
    Função responsável por realizar o upload dos dados extraídos do Five9 nas tabelas de Chat no banco DBDW.

    Args:
        tables_ft (List[str]): Lista de nomes para as tabelas do banco de dados DBDW.

    Returns:
        None
    """
    engine_dw, session_dw = get_connection_from_db("DBDW")
    engine_stg, session_stg = get_connection_from_db("STAGE")

    for table in tables_ft:
        query = f"""
            SELECT
                id_chamada

            FROM DBDW.five9.fato_{table}

            WHERE data_inicio_chamada >= :data_inicio_chamada
        """
        try:
            with engine_dw.connect() as conn, conn.begin():
                df_from_dw = pd.read_sql_query(
                    sql=text(query),
                    con=conn,
                    params={
                        "data_inicio_chamada": (
                            datetime.now() - timedelta(hours=7)
                        ).strftime("%Y-%m-%d %H:%M:%S")
                    },
                )
        except Exception as e:
            logger.error(
                f"Falha na leitura da query da tabela five9_dw.fato_{table} do banco de dados DBDW. Motivo do erro: {e}"
            )
            exit(1)

        if "recebidas" in table:
            query = """
                WITH cte_base_clientes_alloha AS
                (
                SELECT
                    contrato
                    , REPLACE(REPLACE(cidade, CHAR(13), ''), CHAR(10), '') AS cidade
                    , bairro
                    , CASE
                        WHEN marca = 'GIGA+' THEN 'GIGA+ FIBRA'
                        WHEN marca = 'NIU FIBRA' THEN 'NIU'
                        WHEN marca = 'MOB' THEN 'MOBWIRE'
                        ELSE marca
                    END AS marca
                    , REPLACE(polo, 'POLO ', '') AS polo

                FROM DBDW.dbo.base_clientes_alloha

                WHERE CAST(data_referencia AS DATE) >= CAST(DATEADD(HOUR, -3, GETDATE() - 2) AS DATE)
                )
                SELECT DISTINCT
                    CONVERT(VARCHAR, DATEADD(HOUR, -3, GETDATE()), 120) AS data_input
                    , ch.data_extracao
                    , ch.id_chamada
                    , ch.data_inicio_chamada
                    , ch.campanha
                    , ch.agente
                    , ch.tipo_midia
                    , ch.subtipo_midia
                    , ch.resultado
                    , ch.motivo_retencao_ura
                    , ch.cliente
                    , ch.cpf_cnpj
                    , ch.contrato
                    , ch.origem
                    , ch.flg_chat_ou_telefone
                    , IIF(cli.cidade IS NULL, cli_vip.cidade, cli.cidade) AS cidade
                    , ch.cep
                    , IIF(cli.bairro IS NULL, cli_vip.bairro, cli.bairro) AS bairro
                    , ch.pergunta_satisfacao_1
                    , ch.pergunta_satisfacao_2
                    , ch.pergunta_satisfacao_3
                    , ch.tempo_interacao
                    , ch.tempo_conversa
                    , ch.tempo_espera_fila
                    , ch.apos_horario_trabalho
                    , ch.transferida_de
                    , ch.transferida_para
                    , ch.transferencias
                    , ch.marca
                    , ch.polo

                FROM STAGE.five9.stg_chamadas_chat_v2 ch
                LEFT JOIN (SELECT contrato, polo, cidade, bairro FROM cte_base_clientes_alloha WHERE polo <> 'VIP') cli ON ch.contrato = cli.contrato AND
                                                                                                                           ch.polo = cli.polo
                LEFT JOIN (SELECT contrato, marca, cidade, bairro FROM cte_base_clientes_alloha WHERE polo = 'VIP') cli_vip ON ch.contrato = cli_vip.contrato AND
                                                                                                                               ch.marca = cli_vip.marca

                WHERE campanha NOT LIKE '%ativo%'
            """
        else:
            query = """
                SELECT DISTINCT
                    CONVERT(VARCHAR, DATEADD(HOUR, -3, GETDATE()), 120) AS data_input
                    , data_extracao
                    , id_chamada
                    , data_inicio_chamada
                    , campanha
                    , agente
                    , tipo_midia
                    , subtipo_midia
                    , resultado
                    , cliente
                    , origem AS destino
                    , tempo_interacao
                    , tempo_conversa
                    , tempo_espera_fila
                    , apos_horario_trabalho
                    , transferida_de
                    , transferida_para
                    , transferencias
                    , marca
                    , polo

                FROM STAGE.five9.stg_chamadas_chat_v2

                WHERE campanha LIKE '%ativo%'
            """
        try:
            with engine_stg.connect() as conn, conn.begin():
                df_from_stg = pd.read_sql_query(sql=text(query), con=conn)
        except Exception as e:
            logger.error(
                f"Falha na leitura da query vindo da STAGE que irá inserir dados na tabela five9.fato_{table} do banco de dados DBDW. Motivo do erro: {e}"
            )
            exit(1)

        df_filtered = (
            df_from_stg.merge(df_from_dw, on=["id_chamada"], how="left", indicator=True)
            .query("_merge == 'left_only'")
            .drop(columns=["_merge"])
        )

        logger.info(
            f"Total de registros a serem inseridos na tabela five9.fato_{table} do banco de dados DBDW: {df_filtered['id_chamada'].count()}"
        )
        # Carrega os dados filtrados para as tabelas 'fato_chamadas_recebidas_chat' e 'fato_chamadas_ativas_chat' do DW
        load_data(
            data=df_filtered,
            table_name="fato_" + table,
            schema="five9",
            database="DBDW",
            engine=engine_dw,
            session=session_dw,
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
    #     "tma_transferidas",
    # ]
    tables_ft = ["chamadas_recebidas_voz", "chamadas_ativas_voz"]
    # startAt = "2023-10-05T10:59:59.000-03:00"
    # endAt = "2023-10-05T16:59:59.000-03:00"
    # hours = 7

    # lista_df = await upload_to_dbapi_voz(
    #     folder_name, reports_name, tables_stg, startAt, endAt, hours
    # )

    # upload_to_stage_voz(lista_df, tables_stg)

    upload_to_dw_voz(tables_ft)


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
