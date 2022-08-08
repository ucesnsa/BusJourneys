import pandas as pd
import sqlalchemy as db
from sqlalchemy import exc
from pathlib import Path
from mylib import data_model as dm
from mylib import reference_data as rd

class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class OysterJourney(object):
    __metaclass__ = Singleton
    bus_stop_dic = {}

    def __new__(cls, *args, **kwargs):
        print("Initialise OysterJourney class")
        cls.bus_stop_dic = rd.get_bus_stops_dictionary()
        return super().__new__(cls)

    def get_SCD_journeys(self,userid, db_name,verbos=0):
        try:
            lstdays = []
            query = Path("queries\\SCD_Journeys_query.txt").read_text()
            query = query.replace("@userid", userid)
            # get a connection, if a connect cannot be made an exception will be raised here
            engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/'+db_name)
            connection = engine.connect()
            if (verbos == 1):
                print(query)
            ResultSet = connection.execute(query)
            dfJourneys = pd.DataFrame(ResultSet)

            query2 = "SELECT distinct r.prestigeid,  daykey " \
                     "FROM tbl_rawdata r " \
                     "where PRESTIGEID = '" + userid + "' " \
                     "order by daykey"

            if (verbos == 1):
                print(query2)

            #ResultSet = connection.execute(query2)
            # dfdays = pd.DataFrame(ResultSet)

            if dfJourneys.size == 0:
                return dfJourneys, lstdays

            lstdays = dfJourneys['daykey'].unique()
            #print(sorted(lstdays))



            if (verbos == 1):
                print("'" + userid + "'" +" - journey data size " + str(dfJourneys.shape))
                print("'" + userid + "'" + " - journey days size " + str(lstdays.shape))

            return dfJourneys,lstdays
        except exc.SQLAlchemyError:
            exit("Encountered general SQLAlchemyError!")


    def get_all_users(self,db_name, verbos=0):
        try:
            # get a connection, if a connect cannot be made an exception will be raised here
            engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/'+db_name)
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