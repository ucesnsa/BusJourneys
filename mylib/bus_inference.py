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
