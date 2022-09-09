from mylib import data_model as dm


# Start Location Algorithm
# This algorithm looks at 3 consecutive journeys to determine start and end location of bus journeys
# algorithm works on the basis that none of the start or end location for bus journeys are available
# we do have the user's home location data available
# algorithm to infer bus startStation -
# the algorithm works by checking the previous journey if it is a non - bus journey (train) as they capture both start and end location
def infer_bus_info(journey_current, journey_next, journey_prev, user_detail, verbos):
    if journey_prev == None:
        journey_current.StartStationInferred = user_detail.HomeLocation
    elif journey_prev.TransportMode != dm.TransportEnum.BUS.name:
        journey_current.StartStationInferred = journey_prev.EndStation
    elif journey_prev.TransportMode == dm.TransportEnum.BUS.name:
        journey_current.StartStationInferred = 'Undetermined'
    else:
        journey_current.StartStationInferred = 'None'

    # algorithm to infer bus endStation -
    if journey_current.IsLastJourney == True and journey_prev != None:
        journey_current.EndStationInferred = user_detail.HomeLocation
    elif journey_next != None and journey_next.TransportMode != dm.TransportEnum.BUS.name:
        journey_current.EndStationInferred = journey_next.StartStation
    elif journey_next != None and journey_next.TransportMode == dm.TransportEnum.BUS.name:
        journey_current.EndStationInferred = 'Undetermined'
    else:
        journey_current.EndStationInferred = 'None'

    return journey_current


# End Location algorithm without home location

# This algorithm looks at current journey, next journey, last journey, first journey
# it is expected that the start location of the bus journey is also available.
def infer_bus_end_station(journey_current, journey_next, journey_prev, journey_count, journey_first_of_day, verbos):
    # main rule -  part 1
    if journey_next != None:
        journey_current.EndStationInferred = journey_next.StartStation

    # main rule -  part 1
    # if the current journey is the last journey of the day (i.e. journey_next == None ) and user has used the same bus,
    # it can be assumed that
    # user is going in the opposite direction of the first journey of the day. Think about like that if someone
    # is taking bus as the last mile of the journey
    elif journey_current.IsLastJourney == True and journey_first_of_day != None and journey_count > 1 \
            and journey_current.BusNo == journey_first_of_day.BusNo \
            and journey_first_of_day.StartStation is not None \
            and journey_first_of_day.Direction != journey_current.Direction:
        journey_current.EndStationInferred = journey_first_of_day.StartStation

    # implementation of RULE2 with relaxed condition
    # relax the same bus and opposite direction condition concated + (RULE2)
    elif journey_current.IsLastJourney == True and journey_first_of_day != None and journey_count > 1 \
            and journey_first_of_day.StartStation is not None:
        journey_current.EndStationInferred = journey_first_of_day.StartStation + '(RULE2)'

    return journey_current


def infer_shenzhen_bus_end_station(journey_current, journey_next, journey_prev, journey_count, journey_first_of_day, verbos):
    # main rule -  part 1
    if journey_next != None:
        journey_current.EndStationInferred = journey_next.StartStation or journey_next.BusStopLoc

    # main rule -  part 1
    # if the current journey is the last journey of the day (i.e. journey_next == None ) and user has used the same bus,
    # it can be assumed that
    # user is going in the opposite direction of the first journey of the day. Think about like that if someone
    # is taking bus as the last mile of the journey
    elif journey_current.IsLastJourney == True and journey_first_of_day != None and journey_count > 1 \
            and journey_current.BusNo !='' \
            and journey_current.BusNo == journey_first_of_day.BusNo \
            and journey_first_of_day.StartStation is not None:
        journey_current.EndStationInferred = journey_first_of_day.BusStopLoc

    # implementation of RULE2 with relaxed condition
    # relax the same bus and opposite direction condition concated + (RULE2)
    elif journey_current.IsLastJourney == True and journey_first_of_day != None and journey_count > 1 \
            and journey_first_of_day.StartStation is not None:
        journey_current.EndStationInferred = (journey_first_of_day.StartStation or journey_first_of_day.BusStopLoc )+ '(RULE2)'
    return journey_current
