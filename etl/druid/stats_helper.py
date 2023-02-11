from datetime import datetime, timedelta
from typing import Set

from common_query_templates import distinct_opposing_bowler_template, distinct_opposing_batter_template, get_venue_filter
from batting_query_templates import batting_stats_template, get_batting_adversary_filter
from bowling_query_templates import bowling_stats_template, get_bowling_adversary_filter
from fielding_query_templates import fielding_stats_template

from sqlalchemy.engine import create_engine

class StatsExtracter(object):
    def __init__(self, match_id:str, dt:datetime, venue: str, team: str, player_id: str) -> None:
        self.match_id = match_id
        self.dt = dt
        self.venue = venue
        self.team = team
        self.player_id = player_id
        # self.conn = connect(host='localhost', port=8888, path='/druid/v2/sql/', scheme='http')
        # self.curs = Connection.cursor()
        self.engine = create_engine('druid://localhost:8888/druid/v2/sql/')
        self.conn = self.engine.connect()
        self.opposing_bowlers = self.__get_opposing_bowlers()
        self.opposing_batters = self.__get_opposing_batters()

    def __get_opposing_bowlers(self) -> Set[str]:
        druid_query = distinct_opposing_bowler_template.substitute(
            match_id = self.match_id,
            dt = self.dt,
            batter_team = self.team
        )
        return set([row[0] for row in self.conn.exec_driver_sql(druid_query).fetchall()])

    def __get_opposing_batters(self) -> Set[str]:
        druid_query = distinct_opposing_batter_template.substitute(
            match_id = self.match_id,
            dt = self.dt,
            batter_team = self.team
        )
        return set([row[0] for row in self.conn.exec_driver_sql(druid_query).fetchall()])

    def __extract_batting_stats(self, lookbackdays: int, apply_venue_filter: bool, apply_adversery_filter: bool):
        stats_suffix = "_%d_D"%(lookbackdays)
        additional_filters: str = ""
        if apply_venue_filter:
            stats_suffix += "_venue"
            additional_filters += " and %s"%(get_venue_filter(self.venue))
        if apply_adversery_filter:
            stats_suffix += "_versus"
            additional_filters += " and %s"%(get_batting_adversary_filter(self.opposing_bowlers))
        batting_stats_query = batting_stats_template.substitute(
            start_date = (self.dt - timedelta(days=lookbackdays)).strftime('%Y-%m-%d'),
            end_date = self.dt.strftime('%Y-%m-%d'),
            batter_id = self.player_id,
            additional_filters= additional_filters
        )
        result = self.conn.exec_driver_sql(batting_stats_query).fetchone()
        if result is not None:
            return {
                "batting_sr%s"%(stats_suffix) : result[0],
                "batting_avg%s"%(stats_suffix) : result[1]
            }
        else:
            return {
                "batting_sr%s"%(stats_suffix) : 0,
                "batting_avg%s"%(stats_suffix) : 0
            }

    def __extract_bowling_stats(self, lookbackdays: int, apply_venue_filter: bool, apply_adversery_filter: bool):
        stats_suffix = "_%d_D"%(lookbackdays)
        additional_filters: str = ""
        if apply_venue_filter:
            stats_suffix += "_venue"
            additional_filters += " and %s"%(get_venue_filter(self.venue))
        if apply_adversery_filter:
            stats_suffix += "_versus"
            additional_filters += " and %s"%(get_bowling_adversary_filter(self.opposing_batters))
        bowling_stats_query = bowling_stats_template.substitute(
            start_date = (self.dt - timedelta(days=lookbackdays)).strftime('%Y-%m-%d'),
            end_date = self.dt.strftime('%Y-%m-%d'),
            bowler = self.player_id,
            additional_filters= additional_filters
        )
        result = self.conn.exec_driver_sql(bowling_stats_query).fetchone()
        if result is not None:
            return {
                "bowling_sr%s"%(stats_suffix) : result[0],
                "bowling_avg%s"%(stats_suffix) : result[1],
            }
        else:
            return {
                "bowling_sr%s"%(stats_suffix) : 0,
                "bowling_avg%s"%(stats_suffix) : 0
            }

    def __extract_fielding_stats(self, lookbackdays: int):
        stats_suffix = "_%d_D"%(lookbackdays)
        additional_filters: str = ""
        fielding_stats_query = fielding_stats_template.substitute(
            start_date = (self.dt - timedelta(days=lookbackdays)).strftime('%Y-%m-%d'),
            end_date = self.dt.strftime('%Y-%m-%d'),
            fielder_id = self.player_id,
            additional_filters = additional_filters
        )
        result = self.conn.exec_driver_sql(fielding_stats_query).fetchone()
        if result is not None:
            return {
                "fielding_dismissals%s"%(stats_suffix) : result[0],
            }
        else:
            return {
                "fielding_dismissals%s"%(stats_suffix) : 0
            }
           

    def __get_batting_stats(self):
        batting_stats = self.__extract_batting_stats(30, False, False)
        batting_stats = batting_stats |  self.__extract_batting_stats(90, False, False)
        batting_stats = batting_stats |  self.__extract_batting_stats(180, False, False)
        batting_stats = batting_stats |  self.__extract_batting_stats(30*12*5, False, False)
        batting_stats = batting_stats |  self.__extract_batting_stats(30*12*5, True, False)
        batting_stats = batting_stats |  self.__extract_batting_stats(30*12*5, False, True)
        batting_stats = batting_stats |  self.__extract_batting_stats(30*12*5, True, True)
        return batting_stats

    def __get_bowling_stats(self):
        bowling_stats = self.__extract_batting_stats(30, False, False)
        bowling_stats = bowling_stats |  self.__extract_bowling_stats(90, False, False)
        bowling_stats = bowling_stats |  self.__extract_bowling_stats(180, False, False)
        bowling_stats = bowling_stats |  self.__extract_bowling_stats(30*12*5, False, False)
        bowling_stats = bowling_stats |  self.__extract_bowling_stats(30*12*5, True, False)
        bowling_stats = bowling_stats |  self.__extract_bowling_stats(30*12*5, False, True)
        bowling_stats = bowling_stats |  self.__extract_bowling_stats(30*12*5, True, True)
        return bowling_stats

    def __get_fielding_stats(self):
        return self.__extract_fielding_stats(30*12*5)

    
    def get_player_features(self):
        return self.__get_batting_stats() | self.__get_bowling_stats() | self.__get_fielding_stats()



