from mylib import data_model as dm
#Start Location Algorithm
#This algorithm looks at 3 consecutive journeys to determine start and end location of bus journeys
# algorithm works on the basis that none of the start or end location for bus journeys are available
# we do have the user's home location data available
# algorithm to infer bus startStation -
# the algorithm works by checking the previous journey if it is a non - bus journey (train) as they capture both start and end location
def infer_bus_info(journey_current,journey_next, journey_prev, user_detail, verbos):
    if journey_prev == None :
        journey_current.StartStationInferred = user_detail.HomeLocation
    elif journey_prev.TransportMode != dm.TransportEnum.BUS.name :
        journey_current.StartStationInferred = journey_prev.EndStation
    elif journey_prev.TransportMode == dm.TransportEnum.BUS.name :
        journey_current.StartStationInferred = 'Undetermined'
    else:
        journey_current.StartStationInferred = 'None'

    # algorithm to infer bus endStation -
    if journey_current.IsLastJourney == True and journey_prev != None:
        journey_current.EndStationInferred = user_detail.HomeLocation
    elif journey_next != None and journey_next.TransportMode != dm.TransportEnum.BUS.name:
        journey_current.EndStationInferred = journey_next.StartStation
    elif journey_next != None and journey_next.TransportMode == dm.TransportEnum.BUS.name :
        journey_current.EndStationInferred = 'Undetermined'
    else:
        journey_current.EndStationInferred = 'None'

    return journey_current

#End Location algorithm without home location

#This algorithm looks at current journey, next journey, last journey, first journey
# it is expected that the start location of the bus journey is also available.
def infer_bus_end_station(journey_current, journey_next, journey_lastoftheDay, journey_firstoftheDay, verbos):
    #step 1
    if journey_next != None :
        journey_current.end_location == journey_next.start_location

    #step 2
    # if the current journey is the last journey of the day (i.e. journey_next == None ) and user has used the same bus, it can be assumed that
    # user is going in the opposite direction of the first journey of the day. Think about like that if someone is taking bus as the last mile of the journey
    # the same car_id should be used to comeback for the subway again as the first mile of the journey
    elif journey_current == journey_lastoftheDay and journey_firstoftheDay != None and journey_current.busNo == journey_firstoftheDay.BusNo \
            and journey_firstoftheDay.start_location != None:
        journey_current.end_location = journey_firstoftheDay.start_location
    #alternative to step 2 can be following, but we need to have already established user's home address
    # by applying a home location identification algorrithm for each user
    # elif journey_current == journey_lastoftheDay:
    # journey_current.end_location = user.homelocation


