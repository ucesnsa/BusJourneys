def validate(journey_current, journey_next, journey_count, journey_first_of_day, user_homeloc, verbos):
    # main rule -  part 1
    if journey_next != None:
        journey_current.ValidEndStationInferred = ( journey_next.StartStation or journey_next.BusStopLoc ) + '(NextRule)'

    # main rule - based on home location
    elif journey_current.IsLastJourney == True and user_homeloc != None and user_homeloc != '' \
            and user_homeloc != 'NA':
        journey_current.ValidEndStationInferred = user_homeloc  + '(Stage1)'

    # main rule - without home locations
    # if the current journey is the last journey of the day (i.e. journey_next == None ) and user has used the same bus,
    # it can be assumed that
    # user is going in the opposite direction of the first journey of the day. Think about like that if someone
    # is taking bus as the last mile of the journey
    elif journey_current.IsLastJourney == True and journey_first_of_day != None and journey_count > 1 \
            and journey_current.BusNo !='' \
            and journey_current.BusNo == journey_first_of_day.BusNo \
            and journey_first_of_day.StartStation is not None:
        journey_current.ValidEndStationInferred = journey_first_of_day.BusStopLoc + '(Stage2)'

    # implementation of RULE2 with relaxed condition
    # relax the same bus and opposite direction condition concated + (RULE2)
    elif journey_current.IsLastJourney == True and journey_first_of_day != None and journey_count > 1 \
            and journey_first_of_day.StartStation is not None:
        journey_current.ValidEndStationInferred = (journey_first_of_day.StartStation or journey_first_of_day.BusStopLoc )+ '(Stage3)'
    return journey_current
