import pandas as pd
import sqlalchemy as db
from sqlalchemy import exc
from dataclasses import dataclass, replace
from mylib import matched_journeys as mj
from mylib import oyster_journeys as oj
from mylib import data_model as dm



# algorithm to infer bus info
def infer_bus_info(journey_current,journey_next, journey_prev, user_detail, verbos):
    # for current_journey if previous_journey = None, then current_journey.start_location_inferred = Home_loc
    # for current_journey if previous_journey.journey_type= rail or tube, then current_journey.start_location_inferred = previous_journey.end_station
    # for current_journey if previous_journey.journey_type= bus, then current_journey.start_location_inferred = un_determined
    if journey_prev == None :
        journey_current.StartStationInferred = user_detail.HomeLocation
    elif journey_prev.TransportMode != dm.TransportEnum.BUS :
        journey_current.StartStationInferred = journey_prev.EndStation
    elif journey_prev.TransportMode == dm.TransportEnum.BUS :
        journey_current.StartStationInferred = 'Undetermined'
    else:
        journey_current.StartStationInferred = 'None'

    # for current_journey if next_journey = None, then current_journey.end_location_inferred = Home_location
    # for current_journey if next_journey.journey_type= rail or tube, then current_journey.end_location_inferred = next_journey.start_station
    # for current_journey if next_journey.journey_type=bus, then current_journey.end_location_inferred = un_determined
    if journey_next == None :
        journey_current.EndStationInferred = user_detail.HomeLocation
    elif journey_next.TransportMode != dm.TransportEnum.BUS :
        journey_current.EndStationInferred = journey_next.StartStation
    elif journey_next.TransportMode == dm.TransportEnum.BUS :
        journey_current.EndStationInferred = 'Undetermined'
    else:
        journey_current.EndStationInferred = 'None'

    return journey_current

#verbos - use 1 for debugging and 0 for normal run
verbos = 1

# get all users
dfUser = mj.get_all_users(verbos)

# select one user from the list user, and loop through all the users
userid = dfUser['userid'][1]
#userid = '105124208'

# 1. select home and work location of the user from LTDS survey
usr = mj.get_user_detail(userid,verbos)


# 2. get user journeys and unique days
dfJourneys, dfDays = mj.get_LTDS_journeys(userid,verbos)
# dfData = get_SCD_journeys(userid)

# create a list of journey objects
lstJourneys = list()

# 3. select days in order
for index, row in dfDays.iterrows():
    process_date = row['date_key']
    #print ('process_date', process_date)

    # ordered by date and time
    dfJourneys_by_date = pd.DataFrame.copy(dfJourneys.loc[(dfJourneys['date_key'] == process_date)])
    dfJourneys_by_date.reset_index(inplace=True)
    #loop through all the journeys on the selected date
    for index, row in dfJourneys_by_date.iterrows():
        #print (row)
        journey_current = mj.convert_to_Journey(row, verbos=0)

        if index+1 < len(dfJourneys_by_date) -1:
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

            journey_current = infer_bus_info(journey_current,journey_next,journey_prev,usr,verbos)
            #print (journey_current)

        # add new journey to the list
        lstJourneys.append(journey_current)

dfJourneyDataFinal = pd.DataFrame(lstJourneys)
#print (dfJourneyDataFinal)


filename = 'Data_sample.xlsx'
writer = pd.ExcelWriter(filename)

# write dataframe to excel
dfJourneyDataFinal.to_excel(writer)

# save the excel
writer.save()
print("DataFrame is exported successfully to Excel File.")
