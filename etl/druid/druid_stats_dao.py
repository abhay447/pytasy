from datetime import datetime, timedelta
from typing import Set


from etl.druid.druid_query_templates import batting_stats_template, bowling_stats_template, batting_match_stats_template, distinct_opposing_bowler_template, distinct_opposing_batter_template, fielding_match_stats_template, bowling_match_stats_template

from sqlalchemy.engine import create_engine


class DruidStatsDao(object):
    def __init__(self):
        self.engine = create_engine('druid://localhost:8888/druid/v2/sql/')

    def distinct_opposing_bowlers(self, match_id: str,  dt: datetime, batter_team:str) -> Set[str]:
        druid_query = distinct_opposing_bowler_template.substitute(
            match_id = match_id,
            dt = dt,
            batter_team = batter_team
        )
        conn = self.engine.connect()
        return set([str(row[0]) for row in conn.exec_driver_sql(druid_query).fetchall()])

    def distinct_opposing_batters(self, match_id: str,  dt: datetime, bowler_team:str) -> Set[str]:
        druid_query = distinct_opposing_batter_template.substitute(
            match_id = match_id,
            dt = dt,
            bowler_team = bowler_team
        )
        conn = self.engine.connect()
        return set([str(row[0]) for row in conn.exec_driver_sql(druid_query).fetchall()])

    def get_batting_features(self, dt: datetime, player_id:str, lookbackdays: int, additional_filters:str):
        batting_stats_query = batting_stats_template.substitute(
            start_date = (dt - timedelta(days=lookbackdays)).strftime('%Y-%m-%d'),
            end_date = dt.strftime('%Y-%m-%d'),
            batter_id = player_id,
            additional_filters= additional_filters
        )
        conn = self.engine.connect()
        return conn.exec_driver_sql(batting_stats_query).fetchone()

    def get_bowling_features(self, dt: datetime, player_id:str, lookbackdays: int, additional_filters:str):
        bowling_stats_query = bowling_stats_template.substitute(
            start_date = (dt - timedelta(days=lookbackdays)).strftime('%Y-%m-%d'),
            end_date = dt.strftime('%Y-%m-%d'),
            bowler_id = player_id,
            additional_filters= additional_filters
        )
        conn = self.engine.connect()
        return conn.exec_driver_sql(bowling_stats_query).fetchone()

    def get_fielding_features(self, dt: datetime, player_id:str, lookbackdays: int, additional_filters:str):
        fielding_stats_query = fielding_match_stats_template.substitute(
            start_date = (dt - timedelta(days=lookbackdays)).strftime('%Y-%m-%d'),
            end_date = dt.strftime('%Y-%m-%d'),
            fielder_id = player_id,
            additional_filters= additional_filters
        )
        conn = self.engine.connect()
        return conn.exec_driver_sql(fielding_stats_query).fetchone()

    def get_batting_match_stats(self, dt: datetime, player_id:str, match_id: str):
        batting_match_stats_query = batting_match_stats_template.substitute(
            dt=dt,
            match_id=match_id,
            batter_id=player_id
        )
        conn = self.engine.connect()
        return conn.exec_driver_sql(batting_match_stats_query).fetchone()

    def get_bowling_match_stats(self, dt: datetime, player_id:str, match_id: str):
        bowling_match_stats_query = bowling_match_stats_template.substitute(
            dt=dt,
            match_id=match_id,
            bowler_id=player_id
        )
        conn = self.engine.connect()
        return conn.exec_driver_sql(bowling_match_stats_query).fetchone()

    def get_fielding_match_stats(self, dt: datetime, player_id:str, match_id: str):
        fielding_match_stats_query = fielding_match_stats_template.substitute(
            dt=dt,
            match_id=match_id,
            fielder_id=player_id
        )
        conn = self.engine.connect()
        return conn.exec_driver_sql(fielding_match_stats_query).fetchone()