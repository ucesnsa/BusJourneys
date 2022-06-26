import pandas as pd
import sqlalchemy as db
from sqlalchemy import exc
from mylib import data_model as dm

def get_LTDS_journeys(userid, verbos=0):
    try:
        # get a connection, if a connect cannot be made an exception will be raised here
        engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/LTDS_Matched_Journeys')
        connection = engine.connect()

        query = "SELECT r.prestigeid,r.daykey,to_date(r.date_key, 'MM/DD/YYYY') as date_key, r.devicekey, r.routeid, " \
                "r.jnystatus, r.mode, r.station_name as stationofentrykey, r.nlc,r.exit_station_name,  " \
                "r.end_to_end_jny ,r.transactiontime, r.end_time, " \
                "TO_CHAR((r.transactiontime || 'minute')::interval, 'HH24:MI:SS') as start_time," \
                "TO_CHAR((r.end_time || 'minute')::interval, 'HH24:MI:SS') as end_time " \
                "FROM matched_journeys r " \
                "where PRESTIGEID = '" + userid + "'" \
                "order by r.daykey::Integer, r.transactiontime::integer"
        if (verbos == 1):
            print(query)
        ResultSet = connection.execute(query)
        dfJourneys = pd.DataFrame(ResultSet)


        query2 = "SELECT distinct r.prestigeid,  to_date(r.date_key, 'MM/DD/YYYY') as date_key " \
                 "FROM matched_journeys r " \
                 "where PRESTIGEID = '" + userid + "' " \
                 "order by to_date(r.date_key, 'MM/DD/YYYY')"
        if (verbos == 1):
            print(query2)
        ResultSet = connection.execute(query2)
        dfdays = pd.DataFrame(ResultSet)

        if (verbos == 1):
            print("'" + userid + "'" +" - journey data size " + str(dfJourneys.shape))
            print("'" + userid + "'" + " - journey days size " + str(dfdays.shape))

        return dfJourneys,dfdays
    except exc.SQLAlchemyError:
        exit("Encountered general SQLAlchemyError!")


def get_all_users(verbos=0):
    try:
        # get a connection, if a connect cannot be made an exception will be raised here
        engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/LTDS_Matched_Journeys')
        connection = engine.connect()

        query = "SELECT distinct r.prestigeid as UserId FROM matched_journeys r "
        if (verbos == 1):
            print(query)
        ResultSet = connection.execute(query)
        dfUser = pd.DataFrame(ResultSet)

        if (verbos == 1):
            print('User Data size' + str(dfUser.shape))
        return dfUser
    except exc.SQLAlchemyError:
        exit("Encountered general SQLAlchemyError!")


def get_user_detail(userid, verbos=0):
    try:
        # get a connection, if a connect cannot be made an exception will be raised here
        engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/LTDS_Matched_Journeys')
        connection = engine.connect()

        query = "SELECT PRESTIGEID Userid, hhpcout home_location, pwspcout work_location  FROM LTDS " \
                 "where PRESTIGEID = '" + userid + "'"

        if (verbos == 1):
            print(query)
        ResultSet = connection.execute(query)
        dfUserDetail = pd.DataFrame(ResultSet)
        u1 = dm.User(dfUserDetail['userid'][0],dfUserDetail['home_location'][0],dfUserDetail['work_location'][0])

        if (verbos == 1):
            print(u1)
        return u1
    except exc.SQLAlchemyError:
        exit("Encountered general SQLAlchemyError!")

# converts journeys dataframe row to Journey class object
def convert_to_Journey(row, verbos):
    e1 = dm.TransportEnum
    if row['mode'] == 'Tram' :
        e1 = dm.TransportEnum.TRAM
    elif row['mode'] == 'Bus' :
        e1 = dm.TransportEnum.BUS
    elif row['mode'] == 'Rail' :
        e1 = dm.TransportEnum.RAIL

    j = dm.Journey(row['prestigeid'], row['date_key'],e1,row['start_time'], row['end_time'], row['stationofentrykey'], row['exit_station_name'])
    return j