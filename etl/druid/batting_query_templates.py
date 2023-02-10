from string import Template
from typing import List

batting_stats_template = Template("""
with base_data as (
  select * from delivery_records WHERE __time > '$start_date' AND __time < '$end_date'  AND "match_type" = 'T20' $additional_filters
), dismissals as (
  select sum("wicket_sum") as dismissal_count from base_data WHERE "wicket_player_id" = '$batter_id'
)
SELECT
  sum("batter_run_sum")*100.0/sum("delivery_count") as strike_rate,
  sum("batter_run_sum")*1.0/(SELECT sum(dismissal_count) from dismissals) as average
from base_data
where "batter_id" = '$batter_id'
""")


def get_venue_filter(venue_name:str):
    return 'venue_name=\'%s\''%venue_name

def get_adversary_filter(bowler_ids: List[str]):
    return 'bowler_id in (%s)'% ",".join(['\'%s\''%bowler_id for bowler_id in bowler_ids])