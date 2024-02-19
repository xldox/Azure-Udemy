from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus


Base = declarative_base()
tableName = 'pausas_agentes'
schemaName = 'five9'

class AgentBreakReport(Base):
    __tablename__ = tableName
    __table_args__ = {"schema": schemaName}
    id = Column(Integer, primary_key=True)
    id_agente = Column(String)
    agente = Column(String)
    data = Column(String)
    hora = Column(String)
    id_ligacao = Column(String)
    status = Column(String)
    codigo_motivo = Column(String)
    tempo_condicao_agente = Column(String)
    competencia = Column(String)
    data_hora = Column(String)
    data_extracao = Column(String)

    def start(connString):
        db_string = "mssql+pyodbc:///?odbc_connect=%s" % quote_plus(connString)
        engine = create_engine(db_string)
        Session = sessionmaker(bind=engine)
        session = Session()
        Base.metadata.create_all(engine)
        print("Table created on database.\n")

        return tableName, session, engine
