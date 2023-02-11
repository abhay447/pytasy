
from typing import Set


def get_batting_adversary_filter(bowler_ids: Set[str]):
    return 'bowler_id in (%s)'% ",".join(['\'%s\''%bowler_id for bowler_id in bowler_ids])

def get_bowling_adversary_filter(batter_ids: Set[str]):
    return 'batter_id in (%s)'% ",".join(['\'%s\''%batter_id for batter_id in batter_ids])

def get_venue_filter(venue_name:str):
    return 'venue_name=\'%s\''%venue_name