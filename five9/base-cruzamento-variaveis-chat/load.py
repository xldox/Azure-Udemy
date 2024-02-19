# -*- coding: utf-8 -*-
def loadData(tableName, dataFrame, session, engine):
    # load dataFrame on database
    print("\nWait a moment. Data are loading on database.")
    try:
        dataFrame.to_sql(tableName, engine, index=False, if_exists='append', schema='five9')
        print('\nData loaded on database!')
    except Exception as err:
        print(f'\nFail to load data on database: {err}.')

    session.commit()
    session.close()
    print('\nClosed database successfully.')

    return session