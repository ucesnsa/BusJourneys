from mylib import data_model as dm

# algorithm to infer bus info
def infer_bus_info(journey_current,journey_next, journey_prev, user_detail, verbos):
    if journey_prev == None :
        journey_current.StartStationInferred = user_detail.HomeLocation
    elif journey_prev.TransportMode != dm.TransportEnum.BUS.name :
        journey_current.StartStationInferred = journey_prev.EndStation
    elif journey_prev.TransportMode == dm.TransportEnum.BUS.name :
        journey_current.StartStationInferred = 'Undetermined'
    else:
        journey_current.StartStationInferred = 'None'


    if journey_current.IsLastJourney == True and journey_prev != None:
        journey_current.EndStationInferred = user_detail.HomeLocation
    elif journey_next != None and journey_next.TransportMode != dm.TransportEnum.BUS.name:
        journey_current.EndStationInferred = journey_next.StartStation
    elif journey_next != None and journey_next.TransportMode == dm.TransportEnum.BUS.name :
        journey_current.EndStationInferred = 'Undetermined'
    else:
        journey_current.EndStationInferred = 'None'

    return journey_current
