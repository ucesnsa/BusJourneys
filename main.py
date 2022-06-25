import mylib as mlib

def run_all(name):
    dfData = mlib.get_journeys('105020557')
    print (dfData.shape)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run_all('PyCharm')
