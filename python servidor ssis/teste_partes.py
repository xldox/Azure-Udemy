import json
import time
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine


def transformar_dados(row):
    id_nota_fiscal = row["id_nota_fiscal"]
    dados = json.loads(row["itens"])

    salvar = []
    for item in dados:
        for imposto in item["impostos"]:
            novo: dict = {
                "id_nota_fiscal": id_nota_fiscal,
                "descricao_produto": item["descricao"],
                "valor_total_dos_impostos_do_produto": item["valor_total_dos_impostos"],
                "imposto": imposto["imposto"],
                "base_de_calculo": imposto["base_de_calculo"],
                "aliquota": imposto["aliquota"],
                "valor": imposto["valor"],
                "retido": imposto["retido"],
                "impostos_retidos": item["impostos_retidos"],
                "Exigibilidade": imposto["Exigibilidade"],
            }
            salvar.append(novo)

    return pd.DataFrame(salvar)


conexao = (
    # Driver que será utilizado na conexão
    "DRIVER={ODBC Driver 17 for SQL Server};"
    # Host/IP ou nome do servidor
    "SERVER=10.15.16.52;"
    # Porta
    "PORT=1433;"
    # Banco que será utilizado
    "DATABASE=STAGE;"
    # Nome de usuário
    "UID=etl.bi;"
    # Senha
    "PWD=18JaTwkicdA!"
)

tempo_inicial = time.time()

for i in range(5):
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % quote_plus(conexao))

    sql_query = f"""
        SELECT [id_nota_fiscal], [itens]
        FROM [sydle].[stg_notas_fiscais_legado]
        WHERE YEAR(data_emissao_recibo) = 2023 AND
              MONTH(data_emissao_recibo) = {5 - i}"""

    source_df = pd.read_sql(sql_query, engine)




    # Particionando o dataframe fonte ser transformado em pequenas partes de cada vez

    tamanho_parte = 1000  # Processar em lotes de 1000 linhas
    num_partes = (
        len(source_df) // tamanho_parte + 1
        if len(source_df) % tamanho_parte != 0
        else len(source_df) // tamanho_parte
    )
    print(num_partes)
    input("TESTE")
    # Inicializar uma lista para armazenar os DataFrames parciais
    partes_transformadas = []
    print(num_partes)
    for i in range(num_partes):
        inicio = i * tamanho_parte
        fim = min((i + 1) * tamanho_parte, len(source_df))

        parte_df = source_df.iloc[inicio:fim]

        df_transformado_parte = parte_df.apply(transformar_dados, axis=1)

        # Concatenar o DataFrame resultante da parte atual à lista de partes transformadas
        df_transformado_parte = pd.concat(
            df_transformado_parte.tolist(), ignore_index=True
        )
        partes_transformadas.append(df_transformado_parte)

    # Concatenar todos os DataFrames parciais em um único DataFrame final
    df_transformado = pd.concat(partes_transformadas, ignore_index=True)

    df_transformado.to_sql(
        name="stg_nota_fiscal_impostos_legado",
        con=engine,
        schema="sydle",
        if_exists="append",
        index=False,
    )

tempo_final = time.time()

print(f"o codigo demorou {int(tempo_final - tempo_inicial)} segundos para rodar")
