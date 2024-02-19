# -*- coding: utf-8 -*-
import base64, platform, pandas as pd
import os
from datetime import datetime, date, timedelta
from io import StringIO
from model import AgentBreakReport
from extract import getDataAPI
from load import loadData
from dotenv import load_dotenv

load_dotenv()


def str2DataFrame(data):
    # Transform string to DataFrame
    df = pd.read_csv(StringIO(data), sep=",")

    df['data_hora'] = pd.to_datetime(df['DATA'] + ' ' + df['TEMPO']) + timedelta(hours=+4)

    df['data_extracao'] = datetime.today()

    df.rename(columns={'ID DO AGENTE': 'id_agente', 'AGENTE': 'agente', 'DATA': 'data', 'TEMPO': 'hora', 'ID DA LIGAÇÃO': 'id_ligacao', 'ESTADO': 'status',
                       'CÓDIGO DO MOTIVO': 'codigo_motivo', 'TEMPO DE CONDIÇÃO DO AGENTE': 'tempo_condicao_agente', 'COMPETÊNCIA': 'competencia'},
                       inplace=True)

    return df.astype(str)


# Set the correct connection_string parameter for the each OS
if platform.platform()[:7] == 'Windows':
    from connect_string import windows as p
else:
    from connect_string import ubuntu as p

# Create table on database
tableName, session_db, engine_db = AgentBreakReport.start(connString=p)

# Encode credentials Five9 in base64
credentials = os.environ['user'] + ':' + os.environ['password']
b64Val = base64.b64encode(credentials.encode()).decode()

# URL WebService
url = "https://api.five9.com/wsadmin/v13/AdminWebService"

# Set API headers
headers = {
    'Authorization': 'Basic %s' % b64Val,
    'Content-Type': 'application/xml;charset=utf-8',
    'Accept-Encoding': 'gzip, deflate, br',
    'SOAPAction': ''
}

# Set report variables
folderName = 'Meus relatorios'
reportName = 'Report Pausas - Pausas dos agentes'
todayDate  = date.today()
startDay   = int(os.environ['startDay'])
endDay     = int(os.environ['endDay'])


result = getDataAPI(url, headers, folderName, reportName, todayDate, startDay, endDay)

agentBreakReport = str2DataFrame(data=result)

# Call of the function loadData for insert data on database
loadData(tableName=tableName, dataFrame=agentBreakReport, session=session_db, engine=engine_db)