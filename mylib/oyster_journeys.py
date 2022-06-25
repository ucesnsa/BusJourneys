import pandas as pd
import sqlalchemy as db
from sqlalchemy import exc

def get_SCD_journeys(userid, verbos=0):
    try:
        # get a connection, if a connect cannot be made an exception will be raised here
        engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/Oyster')
        connection = engine.connect()
        # metadata = db.MetaData()
        # oyster_data = db.Table('tbl_rawdata', metadata, autoload=True, autoload_with=engine)
        # query = db.select([oyster_data]).where(oyster_data.columns.prestigeid == userid)

        query = "SELECT r.prestigeid,r.daykey,r.date_key,  r.devicekey, r.routeid, r.jnystatus," \
                "r.mode, r.station_name as stationofentrykey, r.nlc,r.exit_station_name,  r.end_to_end_jny ," \
                "r.transactiontime, r.end_time," \
                "TO_CHAR((r.transactiontime || 'minute')::interval, 'HH24:MI:SS') as start_time," \
                "TO_CHAR((r.end_time || 'minute')::interval, 'HH24:MI:SS') as end_time " \
                "FROM matched_journeys r " \
                "where PRESTIGEID = '" + userid + "'"
        print(query)
        ResultSet = connection.execute(query)
        dfJourneys = pd.DataFrame(ResultSet)

        if (verbos == 1):
            print('Data size' + str(dfJourneys.shape))
        return dfJourneys
    except exc.SQLAlchemyError:
        exit("Encountered general SQLAlchemyError!")

