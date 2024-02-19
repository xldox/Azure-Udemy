from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus


Base = declarative_base()
tableName = 'interacoes_digitais'
schemaName = 'five9'

class DigitalInteractionsReport(Base):
    __tablename__ = tableName
    __table_args__ = {"schema": schemaName}
    id = Column(Integer, primary_key=True)
    id_sessao = Column(String)
    data = Column(String)
    hora = Column(String)
    campanha = Column(String)
    usuario = Column(String)
    tipo_midia = Column(String)
    subtipo_midia = Column(String)
    tipo_interacao = Column(String)
    posicao = Column(String)
    nome_cliente = Column(String)
    para_endereco = Column(String)
    do_endereco = Column(String)
    tempo_interacao = Column(String)
    tempo_fila = Column(String)
    apos_horario_trabalho = Column(String)
    transferencias = Column(String)
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