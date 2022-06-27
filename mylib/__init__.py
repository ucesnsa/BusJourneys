import pandas as pd
from mylib import matched_journeys as mj
from mylib import oyster_journeys as oj
from mylib import data_model as dm
from mylib import bus_inference as bi
from mylib import db_utils as du


#verbos - use 1 for debugging and 0 for normal run
verbos = 0

# drop table , used to save the results
du.drop_db_table()
print ('Cleared DB table to sort the results. ')

# get all users
dfUser = mj.get_all_users(verbos)
print ('Processing ', end='')
for ind1, row in dfUser.iterrows():
    print('.', end='')
#    if ind1 == 12:
#        break

    # select one user from the list user, and loop through all the users
    userid = row['userid']

    #userid = dfUser['userid'][1]
    #userid = '518652322'

    # 1. select home and work location of the user from LTDS survey
    usr = mj.get_user_detail(userid,verbos)


    # 2. get user journeys and unique days
    dfJourneys, dfDays = mj.get_LTDS_journeys(userid,verbos)

    # create an empty list for user journeys
    lstJourneys = list()

    # 3. select days in order
    for index1, row in dfDays.iterrows():
        process_date = row['date_key']
        #print ('process_date', process_date)

        # ordered by date and time
        dfJourneys_by_date = pd.DataFrame.copy(dfJourneys.loc[(dfJourneys['date_key'] == process_date)])
        dfJourneys_by_date.reset_index(inplace=True)
        #loop through all the journeys on the selected date
        for index, row in dfJourneys_by_date.iterrows():
            #print (row)
            journey_current = mj.convert_to_Journey(row, verbos=0)
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
                #print('length', len(dfJourneys_by_date))
                #print('Index printing', dfJourneys_by_date.index.tolist())
                #print ('current journey Index', index, 'Next Journey Index:', next_index, 'Prev Journey Index:', prev_index)

                if prev_index != -1:
                    #print(dfJourneys_by_date.iloc[prev_index])
                    journey_prev = mj.convert_to_Journey(dfJourneys_by_date.iloc[prev_index], verbos=0)
                else:
                    journey_prev = None

                if next_index != -1:
                    #print(dfJourneys_by_date.iloc[next_index])
                    journey_next = mj.convert_to_Journey(dfJourneys_by_date.iloc[next_index], verbos=0)
                else:
                    journey_next = None

                journey_current = bi.infer_bus_info(journey_current,journey_next,journey_prev,usr,verbos)
                #print (journey_current)

            # add new journey to the list
            lstJourneys.append(journey_current)

    dfJourneyDataFinal = pd.DataFrame(lstJourneys)
    du.write_to_db_table(dfJourneyDataFinal)
    #print (dfJourneyDataFinal)


#filename = 'Data_sample.xlsx'
#writer = pd.ExcelWriter(filename)

# write dataframe to excel
#dfJourneyDataFinal.to_excel(writer)

# save the excel
#writer.save()
#print("DataFrame is exported successfully to Excel File.")
print (' complete')
print ('Check db table for results')
exit()
