import pandas as pd
import sqlalchemy as db
from sqlalchemy import exc
from pathlib import Path
from mylib import data_model as dm

def get_SCD_journeys(userid, verbos=0):
    try:
        query = Path("queries\\SCD_Journeys_query.txt").read_text()
        query = query.replace("@userid", userid)
        # get a connection, if a connect cannot be made an exception will be raised here
        engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/Oyster')
        connection = engine.connect()
        if (verbos == 1):
            print(query)
        ResultSet = connection.execute(query)
        dfJourneys = pd.DataFrame(ResultSet)

        if (verbos == 1):
            print('Data size' + str(dfJourneys.shape))
        return dfJourneys
    except exc.SQLAlchemyError:
        exit("Encountered general SQLAlchemyError!")


def get_all_users(verbos=0):
    try:
        # get a connection, if a connect cannot be made an exception will be raised here
        engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/Oyster')
        connection = engine.connect()

        query = "SELECT distinct r.prestigeid as UserId FROM tbl_rawdata r limit 100"
        if (verbos == 1):
            print(query)
        ResultSet = connection.execute(query)
        dfUser = pd.DataFrame(ResultSet)

        if (verbos == 1):
            print('User Data size' + str(dfUser.shape))
        return dfUser
    except exc.SQLAlchemyError:
        exit("Encountered general SQLAlchemyError!")

# converts journeys dataframe row to Journey class object
def convert_to_Journey(row, verbos):
    e1 = dm.TransportEnum
    if row['transactiontype'] == '12' :
        e1 = dm.TransportEnum.BUS.name
    elif row['transactiontype']  != '12' :
        e1 = dm.TransportEnum.RAIL.name

    #if (verbos == 1):
    #   print (row['prestigeid'], row['date_key'],e1,row['start_time'], row['end_time'], row['stationofentrykey'], row['exit_station_name'])

    j = dm.Journey(row['prestigeid'], row['calendar_dt'],e1,row['start_time'], row['end_time'], row['start_station_name'], row['exit_station_name'],'NA','NA',False,row['busrouteid'],row['busstopid'],row['direction'])

    if (verbos == 1):
        print(j)
    return j