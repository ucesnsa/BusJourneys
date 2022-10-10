
from mylib import db_utils as du
import pandas as pd
from mylib import data_model as dm
from mylib import data_model as dm
from mylib import bus_inference as bi
from mylib import shenzhen_processor as ott
from mylib import shenzhen_validation as va
from mylib import shenzhen_home_loc as hl

import dataclasses

db_name = 'Shenzhen_SCD'
tbl_name_infer = 'shenzhen_bus_inference'  # inferrence results
tbl_name_validate = 'shenzhen_bus_valid'  # validation table, using subways journeys
oj_obj = ott.ShenzhenProcessor()

#verbos - use 1 for debugging and 0 for normal run
verbos = 0

def run_infer():
    du.drop_db_table(db_name, tbl_name_infer)
    print ('Cleared DB table to sort the results. ')

    # select the users required to run the validation using the TransportEnum
    # inference is done using the bus_rail and bus only users
    # max_user None means , it will run for all users
    dfUser = oj_obj.get_users(db_name,dm.TransportEnum.BUS_RAIL_BUS,15000, verbos)

    #True if you want to run with HomeLocation else False
    # this choice will impact if Stage 1 of the algorithm is used for Inference or not
    run_all('infer', dfUser, True)


def run_valid():
    du.drop_db_table(db_name, tbl_name_validate)
    print ('Cleared DB table to sort the results. ')

    # select the users required to run the validation using the TransportEnum
    # validation is done using train only users
    # max_user None means , it will run for all users
    dfUser = oj_obj.get_users(db_name,dm.TransportEnum.RAIL,12000, verbos)

    #True if you want to run with HomeLocation else False
    # this choice will impact if Stage 1 of the algorithm is used for validation or not
    run_all('valid', dfUser, True)


def run_all(name ,dfUser, runwithHomeloc):
    oj_obj = ott.ShenzhenProcessor()
    print (repr(oj_obj))

    print ('Processing ', end='')
    for ind1, row in dfUser.iterrows():
        print('.', end='')
        # ind1 determines how many users to be processed
        #if ind1 == 10000:
        #    break

        # select one user from the list user, and loop through all the users
        userid = row['user_id']
        if runwithHomeloc == True :
            homeloc = row['homelocation']
        else:
            homeloc = None;


        #print (userid)

        #userid = '10352511'

        # 2. get user journeys and unique days
        dfJourneys, lstDays = oj_obj.get_SCD_journeys(userid, db_name, verbos)
        if len(lstDays) == 0:
            continue

        dfJourneys = dfJourneys.sort_values(['journey_date', 'start_time'])

        # create an empty list for user journeys
        lstJourneys = list()

        # list to store the validation journeys
        lst_valid_journeys = list()

        journey_first_of_day = None
        # 3. select days in order
        for row in lstDays:
            process_date = row
            #print ('process_date', process_date)

            # ordered by date and time
            dfJourneys_by_date = pd.DataFrame.copy(dfJourneys.loc[(dfJourneys['journey_date'] == process_date)])
            dfJourneys_by_date.reset_index(inplace=True)

            # proceed only if there are more than 1 journey in the day's data for the user
            if len(dfJourneys_by_date) <= 1:
                continue

            #loop through all the journeys on the selected date
            for index, row in dfJourneys_by_date.iterrows():
                #print (row)
                journey_current = oj_obj.convert_to_Journey(row, verbos=0)

                # save the first journey of the day for the user
                if index == 0:
                    journey_first_of_day = journey_current

                # check if it is last journey of the day
                if index == len(dfJourneys_by_date) -1:
                    journey_current.IsLastJourney = True

                # get next and previous journey for the inference algorithm
                if index+1 < len(dfJourneys_by_date) :
                    next_index = index + 1
                    journey_next = oj_obj.convert_to_Journey(dfJourneys_by_date.iloc[next_index], verbos=0)
                else:
                    journey_next = None

                if index - 1 >= 0 :
                    prev_index = index - 1
                    journey_prev = oj_obj.convert_to_Journey(dfJourneys_by_date.iloc[prev_index], verbos=0)
                else:
                    journey_prev = None

                # run the bus inference for the bus journeys
                if name == 'infer' and journey_current.TransportMode == dm.TransportEnum.BUS.name:
                    journey_current = bi.infer_shenzhen_bus_end_station(journey_current,journey_next, len(dfJourneys_by_date), journey_first_of_day,homeloc, verbos)
                    lstJourneys.append(journey_current)
                    #print (journey_current)

                # run validation code the subway journeys only
                if name == 'valid' and journey_current.TransportMode != dm.TransportEnum.BUS.name:
                    journey_current2 = dataclasses.replace(journey_current)
                    journey_current2 = va.validate(journey_current2,journey_next, len(dfJourneys_by_date), journey_first_of_day,homeloc, verbos)
                    lst_valid_journeys.append(journey_current2)

            if name == 'infer':
                dfJourneyDataFinal = pd.DataFrame(lstJourneys)
                du.write_to_db_table(dfJourneyDataFinal, db_name, tbl_name_infer)

            if name == 'valid':
                dfValidJourneyDataFinal = pd.DataFrame(lst_valid_journeys)
                du.write_to_db_table(dfValidJourneyDataFinal, db_name,tbl_name_validate)

    print (' complete')
    print ('Check db table for results')
    exit()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # running home location identification algorithm first
    # running for all 2 million users can take hours , output table is Shenzhen_SCD.user_info_infer
    # hl.run_all_home_loc('Shenzhen Home Location')


    # run the main code to for bus inference and validation using the train dataset
    # for validation use the train only dataset
    # for inference of bus end station both datasets
    #run_all('PyCharm')
    run_infer()
    #run_valid()
