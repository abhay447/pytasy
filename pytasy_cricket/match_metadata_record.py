from dataclasses import dataclass
from typing import Optional

@dataclass
class MatchMetadataRecord(object):
    match_id: str
    start_date: str
    event_name: Optional[str]
    match_type: str
    team_type: str
    venue_name: str
    city: Optional[str]
    gender: str
    total_overs: int
    season: str
    toss: str
    team_1: str
    team_2: str
