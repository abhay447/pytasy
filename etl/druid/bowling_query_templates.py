from string import Template
from typing import List

bowling_stats_template = Template("""
with base_data as (
  select * from delivery_records WHERE __time > '$start_date' AND __time < '$end_date'  AND "match_type" = 'T20' $additional_filters
), dismissals as (
  select sum("is_wicket") as dismissal_count from base_data WHERE "bowler_id" = '$bowler_id'
)
SELECT
  sum("delivery_count")*1.0/(SELECT sum(dismissal_count) from dismissals) as strike_rate,
  sum("total_run_sum")*1.0/(SELECT sum(dismissal_count) from dismissals) as average
from base_data
where "bowler_id" = '$bowler_id'
""")


def get_venue_filter(venue_name:str):
    return 'venue_name=\'%s\''%venue_name

def get_adversary_filter(batter_ids: List[str]):
    return 'batter_id in (%s)'% ",".join(['\'%s\''%batter_id for batter_id in batter_ids])