import json
import time
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine

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
    salvar = []

    for id_nota_fiscal, item_str in source_df.values.tolist():
        dados = json.loads(item_str)
        for item in dados:
            for imposto in item["impostos"]:
                novo: dict = {
                    "id_nota_fiscal": id_nota_fiscal,
                    "descricao_produto": item["descricao"],
                    "valor_total_dos_impostos_do_produto": item[
                        "valor_total_dos_impostos"
                    ],
                    "imposto": imposto["imposto"],
                    "base_de_calculo": imposto["base_de_calculo"],
                    "aliquota": imposto["aliquota"],
                    "valor": imposto["valor"],
                    "retido": imposto["retido"],
                    "impostos_retidos": item["impostos_retidos"],
                    "Exigibilidade": imposto["Exigibilidade"],
                }
                salvar.append(novo)

    df = pd.DataFrame(salvar)

    df.to_sql(
        name="stg_nota_fiscal_impostos_legado",
        con=engine,
        if_exists="append",
        index=False,
        schema="sydle",
    )

    tempo_final = time.time()

    print(f"o codigo demorou {int(tempo_final - tempo_inicial)} segundos para rodar")
