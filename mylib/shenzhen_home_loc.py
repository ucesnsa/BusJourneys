import mylib.shenzhen_processor as ott
from mylib import db_utils as du
import pandas as pd
from mylib import bus_inference as bi
from mylib import data_model as dm
import dataclasses


def run_all_home_loc(name):
    oj_obj = ott.ShenzhenProcessor()
    print (repr(oj_obj))

    #verbos - use 1 for debugging and 0 for normal run
    verbos = 0

    # drop table , used to save the results
    db_name = 'Shenzhen_SCD'
    tbl_name_infer = 'user_info_infer'  #inferrence results

    du.drop_db_table(db_name, tbl_name_infer)
    print ('Cleared DB table to sort the results. ')

    # create an empty list for user journeys
    lstUserInfo = list()

    # get all users with train only journeys, since home location algorithm use the first and last location
    # we only use train journeys, as end station for bus journeys are not avialable

    dfUser = oj_obj.get_users(db_name,dm.TransportEnum.BUS_RAIL_RAIL, verbos)
    print ('Processing ', end='')

    for ind1, row in dfUser.iterrows():

        # select one user from the list user, and loop through all the users
        userid = row['user_id']

        #userid = '10352511'

        # 2. get user journeys and unique days
        dfJourneys, lstDays = oj_obj.get_SCD_journeys(userid, db_name, verbos)
        if len(lstDays) == 0:
            continue

        dfJourneys = dfJourneys.sort_values(['journey_date', 'start_time'])

        journey_first_day = None
        journey_last_day = None

        home_found_count = 0
        home_loc = None

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

            journey_first_day = oj_obj.convert_to_Journey(dfJourneys_by_date.iloc[0], verbos=0)
            journey_last_day = oj_obj.convert_to_Journey(dfJourneys_by_date.iloc[len(dfJourneys_by_date) -1], verbos=0)
            if (journey_first_day.StartStation != '' and \
                    journey_first_day.StartStation == journey_last_day.EndStation):
                home_loc = journey_first_day.StartStation
                home_found_count = home_found_count + 1
        #print(home_found_count)
        lstUserInfo.append(dm.ShenzhenUser(userid,home_loc ,'NA' ,home_found_count))

        #print(divmod(ind1, 100)[1])
        if divmod(ind1, 1000)[1] == 0:
            print (ind1)
            dfJourneyDataFinal = pd.DataFrame(lstUserInfo)
            du.write_to_db_table(dfJourneyDataFinal, db_name, tbl_name_infer)
            lstUserInfo = list()

    print (' Complete')
    print ('Check db table for results')
    exit()


# this is for unit testing
if __name__ == '__main__':
    run_all_home_loc('PyCharm')

