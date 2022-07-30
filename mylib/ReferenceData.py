import pandas as pd
from mylib import data_model as dm


def get_bus_stops_dictionary():
    #    import os
    #    cwd = os.getcwd()
    #    print(cwd)
    # create an empty dictionary
    bus_stop_dictionary = {}
    print('loading bus stops..')
    file_name = "..\\data\\bus-stops-10-06-15.csv"
    df = pd.read_csv(file_name)
    print(df.shape)
    for index, row in df.iterrows():
        b_code, b_stop_obj = convert_to_bus_stop(row, verbos=0)
        bus_stop_dictionary[b_code] = b_stop_obj

    return bus_stop_dictionary


# converts journeys dataframe row to Journey class object
def convert_to_bus_stop(row, verbos):
    b = dm.BusStop(
        row['Stop_Code_LBSL'],
        row['Bus_Stop_Code'],
        row['Naptan_Atco'],
        row['Stop_Name'],
        row['Location_Easting'],
        row['Location_Northing'],
        row['Heading'],
        row['Stop_Area'],
        row['Virtual_Bus_Stop'])

    if (verbos == 1):
        print(b)

    if b != None:
        return b.Stop_Code_LBSL, b

