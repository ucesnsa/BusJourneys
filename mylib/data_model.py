from dataclasses import dataclass
import datetime
from enum import Enum

class TransportEnum(Enum):
    BUS = 1
    RAIL = 2
    TRAM = 3

@dataclass(frozen=False, order=True)
class Journey:
    # UserId is the same as the prestigeid
    UserId: str
    JourneyDate: datetime
    TransportMode: TransportEnum
    StartTime: datetime
    EndTime: datetime
    StartStation: str
    EndStation: str
    StartStationInferred: str
    EndStationInferred: str
    IsLastJourney: bool
    BusNo: str
    BusStopId: str
    Direction: bool

@dataclass(frozen=False, order=True)
class JourneyShenzhen(Journey):
    StartStationLoc: str
    EndStationLoc: str
    BusStopLoc: str

@dataclass(frozen=True, order=True)
class User:
    # UserId is the same as the prestigeid
    UserId: str
    HomeLocation: str
    WorkLocation: str



@dataclass(frozen=False, order=True)
class BusStop:
    Stop_Code_LBSL: str
    Bus_Stop_Code: str
    Naptan_Atco: str
    Stop_Name: str
    Location_Easting: str
    Location_Northing: str
    Heading: str
    Stop_Area: str
    Virtual_Bus_Stop: str
