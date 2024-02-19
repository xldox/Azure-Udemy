from sqlalchemy import MetaData, Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus


#Base = declarative_base(metadata=MetaData(schema='five9'))
Base = declarative_base()
tableName = 'chamadas_recebidas_voz'
schemaName = 'five9'

class CallReport(Base):
    __tablename__ = tableName
    __table_args__ = {"schema": schemaName}
    id = Column(Integer, primary_key=True)
    id_ligacao = Column(String)
    data = Column(String)
    hora = Column(String)
    campanha = Column(String)
    competencia = Column(String)
    agente = Column(String)
    nome_agente = Column(String)
    posicao = Column(String)
    ani = Column(String)
    dnis = Column(String)
    tempo_ligacao = Column(String)
    tempo_conversa = Column(String)
    tempo_espera_fila = Column(String)
    transferencias = Column(String)
    abandonada = Column(String)
    cidade = Column(String)
    bairro = Column(String)
    contrato = Column(String)
    massiva = Column(String)
    cpf_cnpj = Column(String)
    marca = Column(String)
    endereco = Column(String)
    pergunta_3 = Column(String)
    pergunta_2 = Column(String)
    pergunta_1 = Column(String)
    data_ligacao = Column(String)
    data_extracao = Column(String)

    def start(connString):
        db_string = "mssql+pyodbc:///?odbc_connect=%s" % quote_plus(connString)
        engine = create_engine(db_string)
        Session = sessionmaker(bind=engine)
        session = Session()
        Base.metadata.create_all(engine)
        print("Table created on database.\n")

        return tableName, session, engine