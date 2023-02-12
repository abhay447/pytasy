from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

def __extract_batting_fantasy_points(batter_runs: int, dismissals: int, balls_faced: int, boundaries_count: int, sixes_count: int ):
    fantasy_points = 0
    is_out: bool = dismissals > 0
    is_duck = batter_runs==0 and is_out
    strike_rate = batter_runs*100.0/balls_faced if balls_faced > 0 else 100
    fantasy_points = batter_runs * 1 + boundaries_count * 1 + sixes_count * 2
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

def __extract_bowling_fantasy_points(total_runs: int, wickets: int, deliveries: int, maidens: int):
    fantasy_points = 0
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

def __extract_fielding_fantasy_points(fielding_wickets: int):
    fantasy_points = 0
    fantasy_points = fielding_wickets * 7 # avergae to 7 to account for various dismisaals mechanisms
    return fantasy_points

def get_fantasy_points(
        batter_runs: int, dismissals: int, balls_faced: int, boundaries_count: int, sixes_count: int,
        total_runs: int, wickets: int, deliveries: int, maidens: int,
        fielding_wickets: int
    ):
    return __extract_batting_fantasy_points(batter_runs, dismissals, balls_faced, boundaries_count, sixes_count) \
        + __extract_bowling_fantasy_points(total_runs, wickets, deliveries, maidens) \
        + __extract_fielding_fantasy_points(fielding_wickets) \
        + 4.0 # 4 points for being selected
    
get_fantasy_points_udf = udf(get_fantasy_points, LongType())