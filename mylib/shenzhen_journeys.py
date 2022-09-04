import pandas as pd
import sqlalchemy as db
from sqlalchemy import exc
from pathlib import Path
from mylib import data_model as dm
from mylib import reference_data as rd
from mylib import db_connection as db

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ShenzhenJourney(object):
    __metaclass__ = Singleton
    db_obj = None

    def __new__(cls, *args, **kwargs):
        print("Initialise Shenzhen Journey class")
        db_name = 'Shenzhen_SCD'
        cls.db_obj = db.DBConnection(db_name)
        return super().__new__(cls)

    def get_SCD_journeys(self,userid, db_name,verbos=0):
        try:
            lstdays = []
            query = Path("queries\\Shenzhen_Journeys_query.txt").read_text()
            query = query.replace("@userid", userid)

            connection = self.db_obj.conn
            if (verbos == 1):
                print(query)
            ResultSet = connection.execute(query)

            dfJourneys = pd.DataFrame(ResultSet)

            if dfJourneys.size == 0:
                return dfJourneys, lstdays

            lstdays = dfJourneys['journey_date'].unique()
            #print(sorted(lstdays))

            if (verbos == 1):
                print("'" + userid + "'" +" - journey data size " + str(dfJourneys.shape))
                print("'" + userid + "'" + " - journey days size " + str(lstdays.shape))

            return dfJourneys, lstdays

        except exc.SQLAlchemyError:
            print("Encountered general SQLAlchemyError!")
            return dfJourneys, lstdays


    def get_all_users(self,db_name, verbos=0):
        try:
            connection = self.db_obj.conn

            query = "SELECT user_id FROM public.shenzhen_users limit 5"
            if (verbos == 1):
                print(query)
            ResultSet = connection.execute(query)
            dfUser = pd.DataFrame(ResultSet)

            if (verbos == 1):
                print('User Data size' + str(dfUser.shape))

            return dfUser
        except exc.SQLAlchemyError:
            print("Encountered general SQLAlchemyError!")
            return dfUser

    # converts journeys dataframe row to Journey class object
    def convert_to_Journey(self,row, verbos):
        e1 = dm.TransportEnum
        if str(row['transport_mode']) == '1':
            e1 = dm.TransportEnum.BUS.name
            start_station_name ='N/A'
        elif str(row['transport_mode']) == '2':
            e1 = dm.TransportEnum.RAIL.name


        #if (verbos == 1):
        #print (row['prestigeid'], row['date_key'],e1,row['start_time'], row['end_time'], row['stationofentrykey'], row['exit_station_name'])

        j = dm.JourneyShenzhen(UserId = row['user_id'],
                               JourneyDate = row['journey_date'],
                               TransportMode= e1,
                               StartTime = row['start_time'],
                               EndTime = row['end_time'],
                               StartStation = row['start_station'],
                               EndStation = row['end_station'],
                               StartStationInferred = 'NA',
                               EndStationInferred = 'NA',
                               IsLastJourney = False,
                               BusNo = row['bus_route_id'],
                               BusStopId = 'NA',
                               Direction = 'N/A',
                               StartStationLoc = row['start_station_lat_long'],
                               EndStationLoc = row['end_station_lat_long'],
                               BusStopLoc = row['bus_start_station_lat_long']
                               )

        if (verbos == 1):
            print(j)
        return j

