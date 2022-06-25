import pandas as pd
import sqlalchemy as db
from sqlalchemy import exc
from dataclasses import dataclass
from mylib import matched_journeys as mj
from mylib import oyster_journeys as oj
from mylib import dataclass as dm

#verbos - use 1 for debugging and 0 for normal run
verbos = 1

# get all users
dfUser = mj.get_all_users(verbos)

# select one user from the list user, and loop through all the users
userid = dfUser['userid'][1]
print ('user',userid)
#userid = '105124208'

# 1. select home and work location of the user from LTDS survey
dfUserDetail = mj.get_user_detail(userid,verbos)
print (dfUserDetail)

# 2. get user journeys and unique days
dfJourneys, dfDays = mj.get_LTDS_journeys(userid,verbos)
# dfData = get_SCD_journeys(userid)

# 3. select days in order
process_date = dfDays['date_key'][1]
print ('process_date', process_date)

# ordered by date and time
dfJourneys_day = dfJourneys.loc[(dfJourneys['date_key'] == process_date)]

# 4. apply bus journey inference algorithm
# select the 1st bus journey as current_journey, and two additional journeys if available (previous_journey and next_journey)
# for current_journey if previous_journey = None, then current_journey.start_location_inferred = Home_loc
# for current_journey if previous_journey.journey_type= rail or tube, then current_journey.start_location_inferred = previous_journey.end_station
# for current_journey if previous_journey.journey_type= bus, then current_journey.start_location_inferred = un_determined

# for current_journey if next_journey = None, then current_journey.end_location_inferred = Home_location
# for current_journey if next_journey.journey_type= rail or tube, then current_journey.end_location_inferred = next_journey.start_station
# for current_journey if next_journey.journey_type=bus, then current_journey.end_location_inferred = un_determined

# select the next bus journey as current_journey, and two additional journeys if available (previous_journey and next_journey)
# continue till no more bus journeys available on the day

filename = 'Data_sample.xlsx'
writer = pd.ExcelWriter(filename)

# write dataframe to excel
dfJourneys_day.to_excel(writer)

# save the excel
writer.save()
print("DataFrame is exported successfully to Excel File.")
