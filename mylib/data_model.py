from dataclasses import dataclass

@dataclass(frozen=True, order=True)
class Journey:
    # UserId is the same as the prestigeid
    UserId: str


class User:
    # UserId is the same as the prestigeid
    UserId: str
    HomeLocation: str
    WorkLocation: str

