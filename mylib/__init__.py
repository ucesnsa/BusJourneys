import pandas as pd
import sqlalchemy as db
from sqlalchemy import exc

def get_journeys(userid, verbos=0):
    try:
        # get a connection, if a connect cannot be made an exception will be raised here
        engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/LTDS_Matched_Journeys')
        connection = engine.connect()
        metadata = db.MetaData()
        MATCHED_JOURNEYS = db.Table('matched_journeys', metadata, autoload=True, autoload_with=engine)
        query = db.select([MATCHED_JOURNEYS]).where(MATCHED_JOURNEYS.columns.prestigeid == '105020557')
        ResultProxy = connection.execute(query)
        ResultSet = ResultProxy.fetchall()
        dfJourneys = pd.DataFrame(ResultSet)

        if (verbos == 1):
            print('Data size' + str(dfJourneys.shape))
        return dfJourneys
    except exc.SQLAlchemyError:
        exit("Encountered general SQLAlchemyError!")
