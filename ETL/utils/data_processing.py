"""Módulo de processamento de dados."""

from typing import List, Union

import pandas as pd


def merge_and_filter_missing_ids(
    df_left: pd.DataFrame, df_right: pd.DataFrame, on_column: Union[str, List[str]]
) -> pd.DataFrame:
    """
    Realiza um merge entre dois DataFrames e filtra as linhas presentes apenas no DataFrame da esquerda.

    Parameters:
    df_left (pandas.DataFrame): O DataFrame da esquerda a ser mesclado.
    df_right (pandas.DataFrame): O DataFrame da direita a ser mesclado.
    on_column (Union[str, List[str]): Coluna(s) para realizar o merge. Pode ser uma string ou uma lista de strings.

    Returns:
    pandas.DataFrame: Um novo DataFrame contendo apenas as linhas do DataFrame da esquerda que não têm correspondência no da direita.
    """
    if isinstance(on_column, str):
        on_column = [on_column]  # Converte uma única string em uma lista de strings

    merged_df = df_left.merge(df_right, on=on_column, how="left", indicator=True)
    filtered_df = merged_df.query("_merge == 'left_only'").drop(columns=["_merge"])

    return filtered_df
