from datetime import datetime
from typing import Dict, Optional, Set, Tuple

from etl.druid.druid_stats_dao import DruidStatsDao
from etl.commons.common_query_templates import get_venue_filter, get_bowling_adversary_filter, get_batting_adversary_filter

from pyspark.sql import Row, SparkSession

from etl.spark.sequential.spark_stats_dao import SparkStatsDao

zero_mapped_values = set(['NaN', 'Infinity'])

class StatsExtracter(object):
    def __init__(self, match_id:str, dt:datetime, venue: str, team: str, player_id: str, player_name: str, spark: Optional[SparkSession], dao_mode: str) -> None:
        self.match_id = match_id
        self.dt = dt
        self.venue = venue.replace("'","''")
        self.team = team
        self.player_id = player_id
        self.player_name = player_name
        if dao_mode == "druid":
            self.dao = DruidStatsDao()
        elif spark is not None:
            self.dao = SparkStatsDao(spark)
        self.opposing_bowlers = self.__get_opposing_bowlers()
        self.opposing_batters = self.__get_opposing_batters()

    def __get_opposing_bowlers(self) -> Set[str]:
        return self.dao.distinct_opposing_bowlers(self.match_id, self.dt, self.team)

    def __get_opposing_batters(self) -> Set[str]:
        return self.dao.distinct_opposing_batters(self.match_id, self.dt, self.team)

    def __extract_batting_features(self, lookbackdays: int, apply_venue_filter: bool, apply_adversery_filter: bool) -> Dict[str,float]:
        stats_suffix = "_%d_D"%(lookbackdays)
        additional_filters: str = ""
        if apply_venue_filter:
            stats_suffix += "_venue"
            additional_filters += " and %s"%(get_venue_filter(self.venue))
        if apply_adversery_filter:
            stats_suffix += "_versus"
            additional_filters += " and %s"%(get_batting_adversary_filter(self.opposing_bowlers))
        result = self.dao.get_batting_features(self.dt,self.player_id,lookbackdays,additional_filters)
        if result is not None:
            # print(result)
            return {
                "feature_batting_sr%s"%(stats_suffix) : float(result[0]) if result[0] is not None and result[0] not in zero_mapped_values else 0.0,
                "feature_batting_avg%s"%(stats_suffix) : float(result[1]) if result[1] is not None and result[1] not in zero_mapped_values else 0.0
            }
        else:
            return {
                "feature_batting_sr%s"%(stats_suffix) : 0.0,
                "feature_batting_avg%s"%(stats_suffix) : 0.0
            }

    def __extract_bowling_features(self, lookbackdays: int, apply_venue_filter: bool, apply_adversery_filter: bool) -> Dict[str,float]:
        stats_suffix = "_%d_D"%(lookbackdays)
        additional_filters: str = ""
        if apply_venue_filter:
            stats_suffix += "_venue"
            additional_filters += " and %s"%(get_venue_filter(self.venue))
        if apply_adversery_filter:
            stats_suffix += "_versus"
            additional_filters += " and %s"%(get_bowling_adversary_filter(self.opposing_batters))
        result = self.dao.get_bowling_features(self.dt,self.player_id,lookbackdays,additional_filters)
        if result is not None:
            return {
                "feature_bowling_sr%s"%(stats_suffix) : float(result[0]) if result[0] is not None and result[0] not in zero_mapped_values else 0.0,
                "feature_bowling_avg%s"%(stats_suffix) : float(result[1]) if result[1] is not None and result[1] not in zero_mapped_values else 0.0,
                "feature_bowling_economy%s"%(stats_suffix) : float(result[2]) if result[2] is not None and result[2] not in zero_mapped_values else 0.0,
            }
        else:
            return {
                "feature_bowling_sr%s"%(stats_suffix) : 0.0,
                "feature_bowling_avg%s"%(stats_suffix) : 0.0,
                "feature_bowling_economy%s"%(stats_suffix) : 0.0,
            }

    def __extract_fielding_features(self, lookbackdays: int) -> Dict[str,float]:
        stats_suffix = "_%d_D"%(lookbackdays)
        additional_filters: str = ""
        result = self.dao.get_fielding_features(self.dt,self.player_id,lookbackdays,additional_filters)
        if result is not None:
            return {
                "feature_fielding_dismissals%s"%(stats_suffix) : float(result[0]) if result[0] is not None else 0.0,
            }
        else:
            return {
                "feature_fielding_dismissals%s"%(stats_suffix) : 0.0
            }
           

    def __get_batting_features(self) -> Dict[str,float]:
        batting_stats = self.__extract_batting_features(30, False, False)
        batting_stats = batting_stats |  self.__extract_batting_features(90, False, False)
        batting_stats = batting_stats |  self.__extract_batting_features(180, False, False)
        batting_stats = batting_stats |  self.__extract_batting_features(30*12*5, False, False)
        batting_stats = batting_stats |  self.__extract_batting_features(30*12*5, True, False)
        batting_stats = batting_stats |  self.__extract_batting_features(30*12*5, False, True)
        batting_stats = batting_stats |  self.__extract_batting_features(30*12*5, True, True)
        return batting_stats

    def __get_bowling_features(self) -> Dict[str,float]:
        bowling_stats = self.__extract_bowling_features(30, False, False)
        bowling_stats = bowling_stats |  self.__extract_bowling_features(90, False, False)
        bowling_stats = bowling_stats |  self.__extract_bowling_features(180, False, False)
        bowling_stats = bowling_stats |  self.__extract_bowling_features(30*12*5, False, False)
        bowling_stats = bowling_stats |  self.__extract_bowling_features(30*12*5, True, False)
        bowling_stats = bowling_stats |  self.__extract_bowling_features(30*12*5, False, True)
        bowling_stats = bowling_stats |  self.__extract_bowling_features(30*12*5, True, True)
        return bowling_stats

    def __get_fielding_features(self)-> Dict[str,float]:
        return self.__extract_fielding_features(30*12*5)

    
    def get_player_features(self) -> Dict[str,float]:
        return self.__get_batting_features() | self.__get_bowling_features() | self.__get_fielding_features()

    
    def __extract_batting_fantasy_points(self):
        fantasy_points = 0
        result = self.dao.get_batting_match_stats(self.dt, self.player_id, self.match_id)
        if result is not None:
            runs: int = result[0] if result[0] is not None else 0
            balls: int = result[1] if result[1] is not None else 0
            is_out: bool = bool(result[2]) if result[2] is not None else False
            boundaries_count: int = result[3] if result[3] is not None else 0
            sixes_count: int = result[4] if result[4] is not None else 0
            is_duck = runs==0 and is_out
            strike_rate = runs*100.0/balls if balls > 0 else 100
            fantasy_points = runs * 1 + boundaries_count * 1 + sixes_count * 2
            # handle half century
            if runs >=50 and runs < 100:
                fantasy_points += 8
            # handle century
            elif runs>=100:
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

    def __extract_bowling_fantasy_points(self):
        fantasy_points = 0
        result = self.dao.get_bowling_match_stats(self.dt,self.player_id,self.match_id)
        if result is not None:
            runs: int = result[0] if result[0] is not None else 0
            balls: int = result[1] if result[1] is not None else 0
            wickets: int = result[2] if result[3] is not None else 0
            maidens: int = result[3] if result[3] is not None else 0
            economy_rate = runs*6.0/balls if balls > 0 else 6
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

    def __extract_fielding_fantasy_points(self):
        fantasy_points = 0
        result = self.dao.get_fielding_match_stats(self.dt, self.player_id,self.match_id)
        if result is not None:
            dismissals: int = result[0] if result[0] is not None else 0
            fantasy_points = dismissals * 7 # avergae to 7 to account for various dismisaals mechanisms
        return fantasy_points

    def get_fantasy_points(self):
        return self.__extract_batting_fantasy_points() \
            + self.__extract_bowling_fantasy_points() \
            + self.__extract_fielding_fantasy_points() \
            + 4.0 # 4 points for being selected
    
    def get_player_match_row(self) -> Row:
        features = self.get_player_features()
        row = Row(
            feature_batting_sr_30_D=features['feature_batting_sr_30_D'],
            feature_batting_avg_30_D=features['feature_batting_avg_30_D'],
            feature_batting_sr_90_D=features['feature_batting_sr_90_D'],
            feature_batting_avg_90_D=features['feature_batting_avg_90_D'],
            feature_batting_sr_180_D=features['feature_batting_sr_180_D'],
            feature_batting_avg_180_D=features['feature_batting_avg_180_D'],
            feature_batting_sr_1800_D=features['feature_batting_sr_1800_D'],
            feature_batting_avg_1800_D=features['feature_batting_avg_1800_D'],
            feature_batting_sr_1800_D_venue=features['feature_batting_sr_1800_D_venue'],
            feature_batting_avg_1800_D_venue=features['feature_batting_avg_1800_D_venue'],
            feature_batting_sr_1800_D_versus=features['feature_batting_sr_1800_D_versus'],
            feature_batting_avg_1800_D_versus=features['feature_batting_avg_1800_D_versus'],
            feature_batting_sr_1800_D_venue_versus=features['feature_batting_sr_1800_D_venue_versus'],
            feature_batting_avg_1800_D_venue_versus=features['feature_batting_avg_1800_D_venue_versus'],
            feature_bowling_sr_30_D=features['feature_bowling_sr_30_D'],
            feature_bowling_avg_30_D=features['feature_bowling_avg_30_D'],
            feature_bowling_economy_30_D=features['feature_bowling_economy_30_D'],
            feature_bowling_sr_90_D=features['feature_bowling_sr_90_D'],
            feature_bowling_avg_90_D=features['feature_bowling_avg_90_D'],
            feature_bowling_economy_90_D=features['feature_bowling_economy_90_D'],
            feature_bowling_sr_180_D=features['feature_bowling_sr_180_D'],
            feature_bowling_avg_180_D=features['feature_bowling_avg_180_D'],
            feature_bowling_economy_180_D=features['feature_bowling_economy_180_D'],
            feature_bowling_sr_1800_D=features['feature_bowling_sr_1800_D'],
            feature_bowling_avg_1800_D=features['feature_bowling_avg_1800_D'],
            feature_bowling_economy_1800_D=features['feature_bowling_economy_1800_D'],
            feature_bowling_sr_1800_D_venue=features['feature_bowling_sr_1800_D_venue'],
            feature_bowling_avg_1800_D_venue=features['feature_bowling_avg_1800_D_venue'],
            feature_bowling_economy_1800_D_venue=features['feature_bowling_economy_1800_D_venue'],
            feature_bowling_sr_1800_D_versus=features['feature_bowling_sr_1800_D_versus'],
            feature_bowling_avg_1800_D_versus=features['feature_bowling_avg_1800_D_versus'],
            feature_bowling_economy_1800_D_versus=features['feature_bowling_economy_1800_D_versus'],
            feature_bowling_sr_1800_D_venue_versus=features['feature_bowling_sr_1800_D_venue_versus'],
            feature_bowling_avg_1800_D_venue_versus=features['feature_bowling_avg_1800_D_venue_versus'],
            feature_bowling_economy_1800_D_venue_versus=features['feature_bowling_economy_1800_D_venue_versus'],
            feature_fielding_dismissals_1800_D=features['feature_fielding_dismissals_1800_D'],
            player_name=self.player_name,
            player_id=self.player_id,
            dt=self.dt.strftime('%Y-%m-%d'),
            venue=self.venue,
            team=self.team,
            fantasy_points=self.get_fantasy_points()
        )
        return row


def map_player_match_for_training(row: Tuple[str,datetime,str,str,str,str]):
    match_id = row[0]
    dt = row[1]
    venue = row[2]
    team = row[3]
    player_id = row[4]
    player_name = row[5]
    stats_extractor = StatsExtracter(match_id=match_id,dt=dt,venue=venue,team=team,player_id=player_id,player_name=player_name, spark=None,dao_mode="druid")
    return stats_extractor.get_player_match_row()

def sequential_player_match_for_training(row: Tuple[str,datetime,str,str,str,str], spark: SparkSession) -> Row:
    match_id = row[0]
    dt = row[1]
    venue = row[2]
    team = row[3]
    player_id = row[4]
    player_name = row[5]
    stats_extractor = StatsExtracter(match_id=match_id,dt=dt,venue=venue,team=team,player_id=player_id,player_name=player_name, spark=spark,dao_mode="spark")
    return stats_extractor.get_player_match_row()