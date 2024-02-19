from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus


Base = declarative_base()
tableName = 'tma'
schemaName = 'five9'

class TMAReport(Base):
    __tablename__ = tableName
    __table_args__ = {"schema": schemaName}
    id = Column(Integer, primary_key=True)
    id_ligacao = Column(String)
    data = Column(String)
    hora = Column(String)
    campanha = Column(String)
    competencia = Column(String)
    ani = Column(String)
    dnis = Column(String)
    segmento = Column(String)
    id_segmento = Column(String)
    chamador = Column(String)
    resultado = Column(String)
    tempo_segmento = Column(String)
    tipo_segmento = Column(String)
    posicao = Column(String)
    tempo_conversa = Column(String)
    tempo_ligacao = Column(String)
    tipo_ligacao = Column(String)
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