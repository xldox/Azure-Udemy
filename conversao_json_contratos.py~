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

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % quote_plus(conexao))
sql_query = """
    SELECT [id_contrato], [componentes]
    FROM [sydle].[stg_contrato]
    WHERE [componentes] IS NOT NULL"""

source_df = pd.read_sql(sql_query, engine)
salvar = []

for id_contrato, componentes_str in source_df.values.tolist():
    dados = json.loads(componentes_str)
    for componentes in dados:
        try:
            for produto_adicional in componentes["precificacao_parcelas"]:
                novo: dict = {
                    "id_contrato": id_contrato,
                    "tarifador": componentes["precificacao_tarifador"],
                    "precificacao_pre_pago": componentes["precificacao_pre_pago"],
                    "precificacao_negociavel": componentes["precificacao_negociavel"],
                    "produto_adicional": componentes["produto_adicional"],
                    "parcela_final": produto_adicional["parcela_final"],
                    "valor_parcela": produto_adicional["valor_parcela"],
                    "parcela_inicial": produto_adicional["parcela_inicial"],
                    "produto_nome": componentes["produto_nome"],
                    "produto_identificador_sydle": componentes[
                        "produto_identificador_sydle"
                    ],
                    "data_de_ativacao": componentes["data_de_ativacao"],
                    "precificacao_pro_rata": componentes["precificacao_pro_rata"],
                    "precificacao_valor": componentes["precificacao_valor"],
                }
                salvar.append(novo)
        except Exception as e:
            novo: dict = {
                "id_contrato": id_contrato,
                "tarifador": componentes["precificacao_tarifador"],
                "precificacao_pre_pago": componentes["precificacao_pre_pago"],
                "precificacao_negociavel": componentes["precificacao_negociavel"],
                "produto_adicional": componentes["produto_adicional"],
                "parcela_final": None,
                "valor_parcela": None,
                "parcela_inicial": None,
                "produto_nome": componentes["produto_nome"],
                "produto_identificador_sydle": componentes[
                    "produto_identificador_sydle"
                ],
                "data_de_ativacao": componentes["data_de_ativacao"],
                "precificacao_pro_rata": componentes["precificacao_pro_rata"],
                "precificacao_valor": componentes["precificacao_valor"],
            }
            salvar.append(novo)
            print(f"Motivo da exceção: {e}")

df = pd.DataFrame(salvar)

df.to_sql(
    name="stg_contrato_componentes",
    con=engine,
    if_exists="append",
    index=False,
    schema="sydle",
    chunksize=1000,
)

tempo_final = time.time()

print(f"o codigo demorou {int(tempo_final-tempo_inicial)} segundos para rodar")
