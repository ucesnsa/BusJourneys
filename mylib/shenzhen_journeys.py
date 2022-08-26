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
        print("Initialise OysterJourney class")
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

            lstdays = dfJourneys['daykey'].unique()
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

            query = "SELECT distinct r.prestigeid as UserId FROM tbl_rawdata r "
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
        if str(row['transactiontype']) == '12':
            e1 = dm.TransportEnum.BUS.name
            try:
                #print(self.bus_stop_dic[str(row['busstopid'])].Stop_Name)
                start_station_name = self.bus_stop_dic[str(row['busstopid'])].Stop_Name
            except KeyError:
                #print ('Key Error',str(row['busstopid']))
                start_station_name ='N/A'
        elif str(row['transactiontype']) != '12':
            e1 = dm.TransportEnum.RAIL.name
            start_station_name = row['start_station_name']

        #if (verbos == 1):
        #   print (row['prestigeid'], row['date_key'],e1,row['start_time'], row['end_time'], row['stationofentrykey'], row['exit_station_name'])

        j = dm.Journey(row['prestigeid'], row['calendar_dt'],e1,row['start_time'], row['end_time'], start_station_name, row['exit_station_name'],'NA','NA',False,row['busrouteid'],row['busstopid'],row['direction'])

        if (verbos == 1):
            print(j)
        return j