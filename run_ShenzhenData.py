import mylib.processor_shenzhen as ott
from mylib import db_utils as du
import pandas as pd
from mylib import bus_inference as bi
from mylib import data_model as dm
from mylib import validation_shenzhen as va
import dataclasses

def run_all(name):
    oj_obj = ott.ShenzhenProcessor()
    print (repr(oj_obj))
#    dfData = ot.get_SCD_journeys('63304617')

    #verbos - use 1 for debugging and 0 for normal run
    verbos = 0

    # drop table , used to save the results


    db_name = 'Shenzhen_SCD'
    tbl_name_infer = 'shenzhen_bus_inference'  #inferrence results
    tbl_name_validate = 'shenzhen_bus_valid'    #validation table, using subways journeys

    du.drop_db_table(db_name, tbl_name_infer)
    du.drop_db_table(db_name, tbl_name_validate)
    print ('Cleared DB table to sort the results. ')

    # get all users
    dfUser = oj_obj.get_all_users(db_name, verbos)
    print ('Processing ', end='')
    for ind1, row in dfUser.iterrows():
        print('.', end='')
        # ind1 determines how many users to be processed
        #if ind1 == 10000:
        #    break

        # select one user from the list user, and loop through all the users
        userid = row['user_id']
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
                if journey_current.TransportMode == dm.TransportEnum.BUS.name:
                    journey_current = bi.infer_shenzhen_bus_end_station(journey_current,journey_next,journey_prev, len(dfJourneys_by_date), journey_first_of_day, verbos)
                    #print (journey_current)

                # run validation code the subway journeys only
                if journey_current.TransportMode != dm.TransportEnum.BUS.name:
                    journey_current2 = dataclasses.replace(journey_current)
                    journey_current2 = va.validate(journey_current2,journey_next,journey_prev, len(dfJourneys_by_date), journey_first_of_day, verbos)
                    lst_valid_journeys.append(journey_current2)
                lstJourneys.append(journey_current)


        dfJourneyDataFinal = pd.DataFrame(lstJourneys)
        du.write_to_db_table(dfJourneyDataFinal, db_name, tbl_name_infer)

        dfValidJourneyDataFinal = pd.DataFrame(lst_valid_journeys)
        du.write_to_db_table(dfValidJourneyDataFinal, db_name,tbl_name_validate)

    print (' complete')
    print ('Check db table for results')
    exit()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
#    db_name = 'scd_bus_journeys'
#    tbl_name = 'bus_inference_scd'

    run_all('PyCharm')
#    du.drop_db_table('SCD_230722', 'test_tbl')
#    oj_obj = ott.OysterJourney()
#    dc = oj_obj.bus_stop_dic
#    print('dc length',len(dc))
#    print('test', dc['26826'].Stop_Name)
#    userid = '63304617'
#    oj_obj.get_SCD_journeys(userid, db_name)
