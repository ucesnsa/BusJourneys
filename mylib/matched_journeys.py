import pandas as pd
import sqlalchemy as db
from sqlalchemy import exc
from dataclasses import dataclass

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

        if (verbos == 1):
            print('User Row' + str(dfUserDetail.shape))
        return dfUserDetail
    except exc.SQLAlchemyError:
        exit("Encountered general SQLAlchemyError!")
