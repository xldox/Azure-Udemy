"""Módulo para ajuste do timezone da data da ligação."""

from datetime import date, datetime, timedelta

import pandas as pd


def finding_first_sunday_of_november(ano: int) -> date:
    """
    Encontra a data do primeiro domingo de novembro para um determinado ano.

    Argumentos:
    - ano (int): O ano para o qual se deseja encontrar o primeiro domingo de novembro.

    Retorna:
    - date: A data correspondente ao primeiro domingo de novembro do ano fornecido.
    """
    data = date(ano, 11, 1)
    dia_semana = data.weekday()
    if dia_semana != 6:
        data += timedelta(days=(6 - dia_semana))

    return data


def finding_second_sunday_of_march(ano: int) -> date:
    """
    Encontra a data do segundo domingo de março para um determinado ano.

    Argumentos:
    - ano (int): O ano para o qual se deseja encontrar o segundo domingo de março.

    Retorna:
    - date: A data correspondente ao segundo domingo de março do ano fornecido.
    """
    data = date(ano, 3, 1)
    dia_semana = data.weekday()
    data += timedelta(days=(7 - dia_semana + 6))

    return data


def get_timezone_correctly() -> int:
    """
    Determina o ajuste correto do fuso horário com base na data atual.

    Imprime a mensagem de ajuste apropriada para o fuso horário (+4 ou +5),
    com base em se a data atual está entre o primeiro domingo de novembro
    e o segundo domingo de março do ano seguinte.
    """
    data_atual = date.today()
    mes_atual = date.today().month
    if mes_atual >= 11 and mes_atual <= 12:
        ano = date.today().year
        primeiro_domingo_novembro = finding_first_sunday_of_november(ano)
        segundo_domingo_marco = finding_second_sunday_of_march(ano + 1)
        if primeiro_domingo_novembro <= data_atual < segundo_domingo_marco:
            return 5
        else:
            return 4
    else:
        ano = date.today().year
        primeiro_domingo_novembro = finding_first_sunday_of_november(ano - 1)
        segundo_domingo_marco = finding_second_sunday_of_march(ano)
        if primeiro_domingo_novembro <= data_atual < segundo_domingo_marco:
            return 5
        else:
            return 4


def converter_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converta as colunas "DATE" e "TIME" de um DataFrame em uma única coluna "data_ligacao" no formato "%Y-%m-%d %H:%M:%S" e ajusta o fuso horário em +4 horas.

    Args:
        df (pd.DataFrame): O DataFrame contendo as colunas "DATE" e "TIME" a serem convertidas.

    Returns:
        pd.DataFrame: O DataFrame com a nova coluna "data_ligacao" no formato desejado.

    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'DATE': ['2023/08/17'], 'TIME': ['09:50:00']})
        >>> result = converter_date(df)
        >>> print(result)
                      DATE         TIME       data_ligacao
              0    2023/08/17    09:50:00    2023-08-17 13:50:00
    """
    # Garantindo que as colunas DATE e TIME tenham valores corretos de data e tempo
    df = df[df.apply(lambda x: len(x["DATE"]) == 10 and len(x["TIME"]) == 8, axis=1)]

    # Combina as colunas "DATE" e "TIME" em uma nova coluna "data_ligacao" e converta para o formato de data e hora
    df["data_ligacao"] = pd.to_datetime(
        df["DATE"] + " " + df["TIME"], format="%Y/%m/%d %H:%M:%S"
    )

    # Adiciona 4 horas a "data_ligacao"
    df["data_ligacao"] = df["data_ligacao"] + pd.Timedelta(
        hours=get_timezone_correctly()
    )

    # Formata a nova coluna 'data_ligacao' no formato de interesse "%Y-%m-%d %H:%M:%S"
    df["data_ligacao"] = df["data_ligacao"].dt.strftime("%Y-%m-%d %H:%M:%S")

    return df


def add_quality_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona colunas de "data_ligacao" e "data_extracao" ao DataFrame.

    Args:
        df (pd.DataFrame): O DataFrame de entrada.

    Returns:
        pd.DataFrame: O DataFrame com as colunas adicionadas.

    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'DATE': ['2023/08/17'], 'TIME': ['09:50:00']})
        >>> result = add_quality_columns(df)
        >>> print(result)
              DATE          TIME        data_ligacao         data_extracao
        0    2023/08/17   09:50:00   2023-08-17 13:50:00   2023-10-24 09:00:00
    """
    # Adiciona colunas de "data_ligacao" e "data_extracao" ao DataFrame
    df = converter_date(df)
    df["data_extracao"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return df
