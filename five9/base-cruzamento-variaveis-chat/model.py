from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus


Base = declarative_base()
tableName = 'base_cruzamento_variaveis_chat'
schemaName = 'five9'

class BaseJoinChatReport(Base):
    __tablename__ = tableName
    __table_args__ = {"schema": schemaName}
    id = Column(Integer, primary_key=True)
    id_ligacao = Column(String)
    data = Column(String)
    hora = Column(String)
    campanha = Column(String)
    cpf_cnpj = Column(String)
    contrato = Column(String)
    codigo = Column(String)
    cidade = Column(String)
    cep = Column(String)
    bairro = Column(String)
    id_sessao = Column(String)
    marca = Column(String)
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