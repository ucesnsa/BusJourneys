import mylib.oyster_journeys as t

def run_all(name):
    dfData = t.get_SCD_journeys('105020557')
    print (dfData.shape)
    t.test()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run_all('PyCharm')
