from dataclasses import dataclass
from typing import Optional


@dataclass
class DeliveryRecord:
    innings_number: int
    batter_name: str
    batter_team: str
    batter_id: str
    bowler_name: str
    bowler_team: str
    bowler_id: str
    batter_runs: int
    extra_runs: int
    total_runs: int
    over: int
    ball: int
    is_wicket: int
    wicket_player_name: Optional[str]
    wicket_player_id: Optional[str]
    wicket_fielder_name: Optional[str]
    wicket_fielder_id: Optional[str]
    wicket_kind: Optional[str]
