from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus


Base = declarative_base()
tableName = 'chamadas_ativas_voz'
schemaName = 'five9'

class ActiveCallReport(Base):
    __tablename__ = tableName
    __table_args__ = {"schema": schemaName}
    id = Column(Integer, primary_key=True)
    id_ligacao = Column(String)
    data = Column(String)
    hora = Column(String)
    campanha = Column(String)
    tipo_ligacao = Column(String)
    agente = Column(String)
    posicao = Column(String)
    nome_cliente = Column(String)
    dnis = Column(String)
    tempo_conversa = Column(String)
    transferencias = Column(String)
    abandonada = Column(String)
    contrato = Column(String)
    cpf_cnpj = Column(String)
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