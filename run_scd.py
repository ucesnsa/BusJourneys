import mylib.processor_london as ott
from mylib import db_utils as du
import pandas as pd
from mylib import bus_inference as bi
from mylib import data_model as dm

def run_all(name):
    oj_obj = ott.OysterJourney()
#    dfData = ot.get_SCD_journeys('63304617')

    #verbos - use 1 for debugging and 0 for normal run
    verbos = 0

    # drop table , used to save the results


    db_name = 'scd_bus_journeys'
    tbl_name = 'bus_inference_scd'

    du.drop_db_table(db_name, tbl_name)
    print ('Cleared DB table to sort the results. ')

    # get all users
    dfUser = oj_obj.get_all_users(db_name, verbos)
    print ('Processing ', end='')
    for ind1, row in dfUser.iterrows():
        print('.', end='')
        # ind1 determines how many users to be processed
        if ind1 == 10000:
            break

        # select one user from the list user, and loop through all the users
        userid = row['userid']
        #print (userid)

        #userid = '10352511'

        # 2. get user journeys and unique days
        dfJourneys, lstDays = oj_obj.get_SCD_journeys(userid, db_name, verbos)
        if len(lstDays) == 0:
            continue

        dfJourneys = dfJourneys.sort_values(['daykey', 'start_time'])

        # create an empty list for user journeys
        lstJourneys = list()

        journey_first_of_day = None
        # 3. select days in order
        for row in lstDays:
            process_date = row
            #print ('process_date', process_date)

            # ordered by date and time
            dfJourneys_by_date = pd.DataFrame.copy(dfJourneys.loc[(dfJourneys['daykey'] == process_date)])
            dfJourneys_by_date.reset_index(inplace=True)
            #loop through all the journeys on the selected date


            for index, row in dfJourneys_by_date.iterrows():
                #print (row)
                journey_current = oj_obj.convert_to_Journey(row, verbos=0)

                # save the first journey of the day for the user
                if index == 0:
                    journey_first_of_day = journey_current

                # proceed only if there are more than 1 journey in the day's data for the user
                if len(dfJourneys_by_date) <= 1:
                    continue

                if index == len(dfJourneys_by_date) -1:
                    journey_current.IsLastJourney = True

                if index+1 < len(dfJourneys_by_date) :
                    next_index = index + 1
                else:
                    next_index = -1

                if index - 1 >= 0 :
                    prev_index = index - 1
                else:
                    prev_index = -1

                if journey_current.TransportMode == dm.TransportEnum.BUS.name:

                    if prev_index != -1:
                        #print(dfJourneys_by_date.iloc[prev_index])
                        journey_prev = oj_obj.convert_to_Journey(dfJourneys_by_date.iloc[prev_index], verbos=0)
                    else:
                        journey_prev = None

                    if next_index != -1:
                        #print(dfJourneys_by_date.iloc[next_index])
                        journey_next = oj_obj.convert_to_Journey(dfJourneys_by_date.iloc[next_index], verbos=0)
                    else:
                        journey_next = None

                    journey_current = bi.infer_bus_end_station(journey_current,journey_next,journey_prev, len(dfJourneys_by_date), journey_first_of_day, verbos)

                    #print (journey_current)

                # add new journey to the list
                lstJourneys.append(journey_current)

        dfJourneyDataFinal = pd.DataFrame(lstJourneys)
        du.write_to_db_table(dfJourneyDataFinal, db_name, tbl_name)

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
