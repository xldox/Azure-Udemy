import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter, And
import json
import itertools
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from parametros import windows as p
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from firebase_admin import firestore
from datetime import datetime
from google.cloud import firestore
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from datetime import datetime
from datetime import datetime, timedelta

#from google.protobuf.timestamp_pb2 import Duration
from google.protobuf.duration_pb2 import Duration


def date_time():
    now = datetime.now()
    print(now)
    data = []
    formatted_date_time = now.strftime('%d-%m-%Y')

    formatted_hour = now.strftime('%H')

    if int(formatted_hour) >= 7:
        print("verdadeiro")

        date_time = '20-12-2023'

        filter_1 = ('createdAt', '>=', f'{date_time} 00:00:00')
        filter_2 = ('createdAt', '<=', f'{date_time} 09:59:59')
        data.append(filter_1)
        data.append(filter_2)


    #print(data)
    return data




def tempo():



    formatted_date_time = '01-10-2023 09:00:00'
    filter_1 = ('createdAt', '>=', f'{formatted_date_time} 00:00:00')
    filter_2 = ('createdAt', '<=', f'{formatted_date_time} 12:00:00')
    data.append(filter_1)
    data.append(filter_2)







def firebase():
    print('Iniciando firebase')
    # cred = credentials.Certificate('C:/Users/João Paulo Souza/PycharmProjects/app-unico/alloha-app-producao-firebase-adminsdk-7k3dp-50f8910ad3.json')
    cred = credentials.Certificate('C:/etl/app-unico/alloha-app-producao-firebase-adminsdk-7k3dp-50f8910ad3.json')
    app = firebase_admin.initialize_app(cred)



def events(data):
    # Initialize Firestore client
    # Define the collection reference
    deadline = 1800.0
    db = firestore.client()
    doc_ref = db.collection('events')
    #now = datetime.now() - timedelta(days=1)
    #formatted_date_time = now.strftime('%d-%m-%Y')
    #filter_1 = ('createdAt', '>=', '03-10-2023 00:00:00')
    #filter_2 = ('createdAt', '<=', '03-10-2023 23:59:59')

    filter_1 = data[0]
    filter_2 = data[1]
    #filter_1 = ('createdAt', '>=', f'{formatted_date_time} 00:00:00')
    #filter_2 = ('createdAt', '<=', f'{formatted_date_time} 23:59:59')
    print(filter_1)
    print(filter_2)
    #input("TESTE")
    # Define the filters

    #filter_1 = ('createdAt', '>=', '27-09-2023 00:00:00')
    #filter_2 = ('createdAt', '<=', '27-09-2023 23:59:59')
    # Construct the query
    #query = doc_ref.where(*filter_1).where(*filter_2)
    docs = doc_ref.where(*filter_1).where(*filter_2).stream(timeout=deadline)
    # Execute the query and retrieve the documents
    #docs = query.stream()
    # Iterate over the documents and append them to a list
    list_of_dict = []
    for doc in docs:
        list_of_dict.append(doc.to_dict())
    data = list_of_dict
    #print('list of dicts',list_of_dict)
    #print('list of dicts',list_of_dict[0])
    df = pd.DataFrame.from_records(data=data)
    print(df)
    engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % quote_plus(p))
    Session = sessionmaker(bind=engine)
    session = Session()
    session.execute(text('''TRUNCATE TABLE app.stg_events'''))
    session.commit()
    session.close()
    #input("TESTE")
    df['params'] = df['params'].apply(json.dumps)
    df['payload'] = df['payload'].apply(json.dumps)
    table_name = 'stg_events'
    #df.to_csv('C:/Users/joao_psouza/Desktop/firebase.csv')
    #input("TESTE")
    df= df.drop_duplicates()
    df.to_sql(table_name, con=engine, schema='app', if_exists='append', index=False, chunksize=1000)
    engine.dispose()
    data = table_name
    destination_table = str('ft' + data)
    print(data)
    destination_table = destination_table.replace('stg', '')
    print('DESTINO', destination_table)
    #input("TESTE")
    print(destination_table)
    engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % quote_plus(p))
    sql_query = (f"SELECT * "
                 f"FROM app.{data};")
    print(sql_query)
    source_df = pd.read_sql(sql_query, engine)
    print(source_df)
    # source_df.to_sql(destination_table, engine, if_exists='replace', index=False)
    source_df.to_sql(destination_table, con=engine, schema='app', if_exists='append', index=False, chunksize=1000)



def sessions(data):
    print('')
    deadline = 900.0
    db = firestore.client()
    doc_ref = db.collection('sessions')
    print(data)
    print(data[0])
    print(data[1])

    # now = datetime.now() - timedelta(days=1)
    # filter_1 = FieldFilter("createdAt", ">=", '11-09-2023 11:00:00')
    # sessions_ref.where('createdAt', '>=', start_timestamp).where('createdAt', '<=', end_timestamp)
    # formatted_date_time = now.strftime('%d-%m-%Y')
    # filter_1 = ('createdAt', '>=', f'{formatted_date_time} 00:00:00')
    # filter_2 = ('createdAt', '<=', f'{formatted_date_time} 23:59:59')

    # Define the filters
    filter = ('createdAt', '>', '04-10-2023 09:00:00')

    if filter == data[0]:
        print("VERDADEIRO")

    # filter_1 = ('createdAt', '>=', '03-10-2023 00:00:00')
    # filter_2 = ('createdAt', '<=', '03-10-2023 23:59:59')

    filter_1 = data[0]
    filter_2 = data[1]
    # filter_1 = ('createdAt', '>=', f'{formatted_date_time} 00:00:00')
    # filter_2 = ('createdAt', '<=', f'{formatted_date_time} 23:59:59')
    print(filter_1)
    print(filter_2)

    # Construct the query
    # query = doc_ref.where(*filter_1).where(*filter_2)
    #docs = doc_ref.where(*filter_1).where(*filter_2).stream()
    docs = doc_ref.where(*filter_1).where(*filter_2).stream(timeout=deadline)
    # Construct the query
    # query = doc_ref.where(*filter_1).where(*filter_2)


    #query = doc_ref.where(*filter_1).where(*filter_2).limit(300)
    #docs = query.stream()

    print('DATASSSSSS',docs)
    #docs = doc_ref.where(filter=filter_1).stream()
    list_of_dict = []
    list_of_dict1 = []
    list_of_final = []
    for doc in docs:
        # list_of_dict.append(doc.to_dict())
        list_of_dict.append(doc.to_dict())
        list_of_dict1.append({'session_id': doc.id})
        merged_list = []
        merged_list.extend(list_of_dict)
        merged_list.extend(list_of_dict1)
        merged_dict = {k: v for d in merged_list for k, v in d.items()}
        list_of_final.append(merged_dict)
    data = list_of_final
    #data = list_of_dict
    # print('list of dicts',list_of_dict)
    # print('list of dicts',list_of_dict[0])
    df = pd.DataFrame.from_records(data=data)
    print(df)
    engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % quote_plus(p))
    Session = sessionmaker(bind=engine)
    session = Session()
    session.execute(text('''TRUNCATE TABLE app.stg_sessions'''))
    #input("teste")
    session.commit()
    session.close()
    #########################
    table_name = 'stg_sessions'
    #df.to_csv('C:/Users/joao_psouza/Desktop/firebase_sessions.csv')
    #input("TESTE")
    #input("AQUI")
    df.to_sql(table_name, con=engine, schema='app', if_exists='append', index=False, chunksize=1000)
    engine.dispose()
    data = table_name
    destination_table = str('ft' + data)
    print(data)
    destination_table = destination_table.replace('stg', '')
    print('DESTINO', destination_table)
    #input("TESTE")
    print(destination_table)
    engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % quote_plus(p))
    sql_query = f"SELECT * FROM app.{data};"
    print(sql_query)
    source_df = pd.read_sql(sql_query, engine)
    print(source_df)
    # source_df.to_sql(destination_table, engine, if_exists='replace', index=False)
    source_df.to_sql(destination_table, con=engine, schema='app', if_exists='append', index=False, chunksize=1000)





data = []
data = date_time()
print(data)


firebase()
sessions(data)
#events(data)
