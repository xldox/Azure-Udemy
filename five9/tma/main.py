# -*- coding: utf-8 -*-
import base64, platform, pandas as pd
import os
from datetime import datetime, date, timedelta
from io import StringIO
from model import TMAReport
from extract import getDataAPI
from load import loadData
from dotenv import load_dotenv

load_dotenv()


def str2DataFrame(data):
    # Transform string to DataFrame
    df = pd.read_csv(StringIO(data), sep=",")

    df['data_ligacao'] = pd.to_datetime(df['DATA'] + ' ' + df['TEMPO']) + timedelta(hours=+4)

    df['data_extracao'] = datetime.today()

    df.rename(columns={'ID DA LIGAÇÃO': 'id_ligacao', 'DATA': 'data', 'TEMPO': 'hora', 'CAMPANHA': 'campanha', 'COMPETÊNCIA': 'competencia', 'ANI': 'ani',
                       'DNIS': 'dnis', 'PESSOA CHAMADA': 'segmento', 'ID DO SEGMENTO DA LIGAÇÃO': 'id_segmento', 'CHAMADOR': 'chamador', 'RESULTADO': 'resultado',
                       'TEMPO DO SEGMENTO': 'tempo_segmento', 'TIPO DO SEGMENTO': 'tipo_segmento', 'POSIÇÃO': 'posicao', 'TEMPO DE CONVERSA': 'tempo_conversa',
                       'TEMPO DE LIGAÇÃO': 'tempo_ligacao', 'TIPO DE LIGAÇÃO': 'tipo_ligacao', 'TRANSFERÊNCIAS': 'transferencias'},
                       inplace=True)

    return df.astype(str)


# Set the correct connection_string parameter for the each OS
if platform.platform()[:7] == 'Windows':
    from connect_string import windows as p
else:
    from connect_string import ubuntu as p

# Create table on database
tableName, session_db, engine_db = TMAReport.start(connString=p)

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
reportName = 'Report - TMA'
todayDate  = date.today()
startDay   = int(os.environ['startDay'])
endDay     = int(os.environ['endDay'])


result = getDataAPI(url, headers, folderName, reportName, todayDate, startDay, endDay)

tmaReport = str2DataFrame(data=result)

# Call of the function loadData for insert data on database
loadData(tableName=tableName, dataFrame=tmaReport, session=session_db, engine=engine_db)