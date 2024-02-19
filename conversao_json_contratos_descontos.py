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
            for dados_descontos in componentes["dados_desconto"]:
                for descontos in componentes["desconto"]:
                    precificacao_nome = descontos["precificacao_nome"]
                    novo: dict = {
                        "id_contrato": id_contrato,
                        "produto_nome": componentes["produto_nome"],
                        "produto_identificador_sydle": componentes[
                            "produto_identificador_sydle"
                        ],
                        "desconto_precificacao_pre_pago": descontos[
                            "precificacao_pre_pago"
                        ],
                        "desconto_precificacao_negociavel": descontos[
                            "precificacao_negociavel"
                        ],
                        "desconto_precificacao_parcela_inicial": descontos[
                            "precificacao_parcela_inicial"
                        ],
                        "desconto_precificacao_parcela_final": descontos[
                            "precificacao_parcela_final"
                        ],
                        "desconto_precificacao_pro_rata": descontos[
                            "precificacao_pro_rata"
                        ],
                        "desconto_nome": descontos["nome"],
                        "desconto_precificacao_nome": precificacao_nome["pt"],
                        "desconto_precificacao_valor": descontos["precificacao_valor"],
                        "desconto_identificador": descontos["identificador"],
                        "data_inicio": dados_descontos["data_inicio"],
                        "data_fim_fidelidade": dados_descontos["data_fim_fidelidade"],
                        "data_termino": dados_descontos["data_termino"],
                    }
                    salvar.append(novo)
        except Exception as e:
            novo: dict = {
                "id_contrato": id_contrato,
                "produto_nome": componentes["produto_nome"],
                "produto_identificador_sydle": componentes[
                    "produto_identificador_sydle"
                ],
                "desconto_precificacao_pre_pago": None,
                "desconto_precificacao_negociavel": None,
                "desconto_precificacao_parcela_inicial": None,
                "desconto_precificacao_parcela_final": None,
                "desconto_precificacao_pro_rata": None,
                "desconto_nome": None,
                "desconto_precificacao_nome": None,
                "desconto_precificacao_valor": None,
                "desconto_identificador": None,
                "data_inicio": None,
                "data_fim_fidelidade": None,
                "data_termino": None,
            }
            salvar.append(novo)
            print(f"Motivo da exceção: {e}")

df = pd.DataFrame(salvar)

df.to_sql(
    name="stg_contrato_descontos",
    con=engine,
    if_exists="append",
    index=False,
    schema="sydle",
)

tempo_final = time.time()

print(f"o codigo demorou {int(tempo_final-tempo_inicial)} segundos para rodar")
