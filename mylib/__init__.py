import pandas as pd
import sqlalchemy as db
from sqlalchemy import exc
from dataclasses import dataclass
from mylib import matched_journeys as mj
from mylib import oyster_journeys as oj
from mylib import dataclass as dm


# select user
userid = '105124208'

# 1. select home and work location of the user from LTDS survey

# 2. get user journeys
dfData = mj.get_LTDS_journeys(userid)
# dfData = get_SCD_journeys(userid)
print(dfData.shape)

# 3. select days in order

# 4. select 1 day and order the journeys by the start time

# 5. apply bus journey inference

# a) select the first bus journey
# start location and time inference
# b) if there is there is a tube or train journey before then use the end station
# c if there is not journey before the bus journey on the day use the home station as the start location

# if there is no journey after this, select the home location as the end location


filename = 'Data_sample.xlsx'
writer = pd.ExcelWriter(filename)

# write dataframe to excel
dfData.to_excel(writer)

# save the excel
writer.save()
print("DataFrame is exported successfully to Excel File.")
