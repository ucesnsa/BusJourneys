import mylib.oyster_journeys as t

def run_all(name):
    dfData = t.get_SCD_journeys('63304617')
    print(dfData.shape)
    for index, row in dfData.iterrows():
        journey_current = t.convert_to_Journey(row, verbos=0)
        print(journey_current)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run_all('PyCharm')
