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


class ShenzhenProcessor(object):
    def __repr__(self):
        return 'Shenzhen Processor'

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

    # this is used for inference and validation
    def get_users_homeloc(self, db_name,journey_type, verbos=0):
        try:
            connection = self.db_obj.conn
            # select users that have journeys of the type
            # train (2) , bus and train combination ('1,2' and '2,1')
            # bus only users 1 are excluded, because not possible to get the home location,
            # if the end location is not avialable
            query = "SELECT user_id FROM public.shenzhen_users shenzhen_users "

            if journey_type == dm.TransportEnum.RAIL:
                journey_type_code = "where string_agg in ('2') "
            elif journey_type == dm.TransportEnum.BUS_RAIL_ONLY:
                journey_type_code = "where string_agg in ('1,2','2,1')"
            elif journey_type == dm.TransportEnum.BUS_RAIL_RAIL:
                journey_type_code = "where string_agg in ('1,2','2,1','2')"
            else:
                journey_type_code = ""

            query = query + journey_type_code

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

    # this is only used in the identification of home location
    # use max_user to limit the users returned , use -1 or all users
    def get_users(self, db_name,journey_type,max_user, verbos=0):
        try:
            connection = self.db_obj.conn
            # select users that have journeys of the type
            # train (2) , bus and train combination ('1,2' and '2,1')
            # bus only users 1 are excluded, because not possible to get the home location,
            # if the end location is not avialable

            query = "SELECT a.user_id, b.homeLocation,a.string_agg FROM public.shenzhen_users a " \
                    "inner join public.user_info_infer b on a.user_id = b.UserId " \
                    "where b.homeFoundCount > 3 "

            if journey_type == dm.TransportEnum.RAIL:
                journey_type_code = "and string_agg in ('2') "
            elif journey_type == dm.TransportEnum.BUS_RAIL_ONLY:
                journey_type_code = "and string_agg in ('1,2','2,1') "
            elif journey_type == dm.TransportEnum.BUS_RAIL_RAIL:
                journey_type_code = "and string_agg in ('1,2','2,1','2') "
            elif journey_type == dm.TransportEnum.BUS_RAIL_BUS:
                journey_type_code = "and string_agg in ('1,2','2,1','1') "
            else:
                journey_type_code = ""

            if max_user != None and max_user > 0 :
                query = query +  journey_type_code + ' limit ' + str(max_user)
            else:
                query = query + journey_type_code

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
                               BusStopLoc = row['bus_start_station_lat_long'],
                               ValidEndStationInferred='NA'
                               )

        if (verbos == 1):
            print(j)
        return j

