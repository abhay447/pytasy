from datetime import datetime, timedelta
from typing import Set


from etl.spark.spark_query_templates import batting_stats_template, bowling_stats_template, batting_match_stats_template, distinct_opposing_bowler_template, distinct_opposing_batter_template, fielding_match_stats_template, bowling_match_stats_template, fielding_stats_template

from pyspark.sql import SparkSession


class SparkStatsDao(object):
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def distinct_opposing_bowlers(self, match_id: str,  dt: datetime, batter_team:str) -> Set[str]:
        druid_query = distinct_opposing_bowler_template.substitute(
            match_id = match_id,
            dt = dt,
            batter_team = batter_team
        )
        return set([str(row[0]) for row in self.spark.sql(druid_query).collect()])

    def distinct_opposing_batters(self, match_id: str,  dt: datetime, bowler_team:str) -> Set[str]:
        druid_query = distinct_opposing_batter_template.substitute(
            match_id = match_id,
            dt = dt,
            bowler_team = bowler_team
        )

        return set([str(row[0]) for row in self.spark.sql(druid_query).collect()])

    def get_batting_features(self, dt: datetime, player_id:str, lookbackdays: int, additional_filters:str):
        batting_stats_query = batting_stats_template.substitute(
            start_date = (dt - timedelta(days=lookbackdays)).strftime('%Y-%m-%d'),
            end_date = dt.strftime('%Y-%m-%d'),
            batter_id = player_id,
            additional_filters= additional_filters
        )
        # print(batting_stats_query)
        result = self.spark.sql(batting_stats_query).collect()
        return result[0] if len(result) > 0 else None

    def get_bowling_features(self, dt: datetime, player_id:str, lookbackdays: int, additional_filters:str):
        bowling_stats_query = bowling_stats_template.substitute(
            start_date = (dt - timedelta(days=lookbackdays)).strftime('%Y-%m-%d'),
            end_date = dt.strftime('%Y-%m-%d'),
            bowler_id = player_id,
            additional_filters= additional_filters
        )
        result = self.spark.sql(bowling_stats_query).collect()
        return result[0] if len(result) > 0 else None

    def get_fielding_features(self, dt: datetime, player_id:str, lookbackdays: int, additional_filters:str):
        fielding_stats_query = fielding_stats_template.substitute(
            start_date = (dt - timedelta(days=lookbackdays)).strftime('%Y-%m-%d'),
            end_date = dt.strftime('%Y-%m-%d'),
            fielder_id = player_id,
            additional_filters= additional_filters
        )
        result = self.spark.sql(fielding_stats_query).collect()
        return result[0] if len(result) > 0 else None

    def get_batting_match_stats(self, dt: datetime, player_id:str, match_id: str):
        batting_match_stats_query = batting_match_stats_template.substitute(
            dt=dt,
            match_id=match_id,
            batter_id=player_id
        )
        result = self.spark.sql(batting_match_stats_query).collect()
        return result[0] if len(result) > 0 else None

    def get_bowling_match_stats(self, dt: datetime, player_id:str, match_id: str):
        bowling_match_stats_query = bowling_match_stats_template.substitute(
            dt=dt,
            match_id=match_id,
            bowler_id=player_id
        )
        result = self.spark.sql(bowling_match_stats_query).collect()
        return result[0] if len(result) > 0 else None

    def get_fielding_match_stats(self, dt: datetime, player_id:str, match_id: str):
        fielding_match_stats_query = fielding_match_stats_template.substitute(
            dt=dt,
            match_id=match_id,
            fielder_id=player_id
        )
        result = self.spark.sql(fielding_match_stats_query).collect()
        return result[0] if len(result) > 0 else None