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
    StartTime:datetime
    EndTime:datetime
    StartStation:str
    EndStation:str
    StartStationInferred:str
    EndStationInferred:str
    IsLastJourney:bool
    BusNo:str

@dataclass(frozen=True, order=True)
class User:
    # UserId is the same as the prestigeid
    UserId: str
    HomeLocation: str
    WorkLocation: str

