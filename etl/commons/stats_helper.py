from typing import Optional
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

def __extract_batting_fantasy_points(
        batter_runs: Optional[int], dismissals: Optional[int],
        balls_faced: Optional[int], boundaries_count: Optional[int], sixes_count: Optional[int] ):
    fantasy_points = 0
    is_out: bool = dismissals is not None and dismissals > 0
    is_duck = batter_runs is not None and batter_runs==0 and is_out
    if batter_runs is None:
        batter_runs = 0
    if balls_faced is None:
        balls_faced = 0
    
    strike_rate = batter_runs*100.0/balls_faced if balls_faced > 0 else 100
    fantasy_points = batter_runs * 1 
    if boundaries_count is not None:
        fantasy_points += boundaries_count * 1
    if sixes_count is not None:
        fantasy_points += sixes_count * 2
    # handle half century
    if batter_runs >=50 and batter_runs < 100:
        fantasy_points += 8
    # handle century
    elif batter_runs>=100:
        fantasy_points += 16
    # strike rate penalty
    if strike_rate >=60 and strike_rate<=70:
        fantasy_points -= 2
    elif strike_rate >=50 and strike_rate<60:
        fantasy_points -= 4
    elif strike_rate <50:
        fantasy_points -= 6
    # duck penalty
    if is_duck:
        fantasy_points -= 2
    return fantasy_points

def __extract_bowling_fantasy_points(total_runs: Optional[int], wickets: Optional[int], deliveries: Optional[int], maidens: Optional[int]):
    fantasy_points = 0
    if total_runs is None:
        total_runs = 0
    if deliveries is None:
        deliveries = 0
    if wickets is None:
        wickets = 0
    if maidens is None:
        maidens = 0
    economy_rate = total_runs*6.0/deliveries if deliveries > 0 else 6
    fantasy_points = wickets * 25 + maidens * 8
    # handle 4 wickets
    if wickets >=4 and wickets < 5:
        fantasy_points += 8
    # handle 5 wickets
    elif wickets>5:
        fantasy_points += 16
    # handle economy bonus
    if economy_rate >= 5 and economy_rate < 6:
        fantasy_points += 2
    elif economy_rate < 5:
        fantasy_points += 4
    return fantasy_points

def __extract_fielding_fantasy_points(fielding_wickets: Optional[int]):
    fantasy_points = fielding_wickets * 7 if fielding_wickets is not None else 0 # avergae to 7 to account for various dismisaals mechanisms
    return fantasy_points

def get_fantasy_points(
        batter_runs: Optional[int], dismissals: Optional[int], balls_faced: Optional[int], boundaries_count: Optional[int], 
        sixes_count: Optional[int],
        total_runs: Optional[int], wickets: Optional[int], deliveries: Optional[int], maidens: Optional[int],
        fielding_wickets: Optional[int]
    ):
    return __extract_batting_fantasy_points(batter_runs, dismissals, balls_faced, boundaries_count, sixes_count) \
        + __extract_bowling_fantasy_points(total_runs, wickets, deliveries, maidens) \
        + __extract_fielding_fantasy_points(fielding_wickets) \
        + 4.0 # 4 points for being selected
    
get_fantasy_points_udf = udf(get_fantasy_points, LongType())