from string import Template
from typing import Set

bowling_stats_template = Template("""
with base_data as (
  select * from delivery_records WHERE __time > '$start_date' AND __time < '$end_date' AND "bowler_id" = '$bowler_id' AND "match_type" = 'T20' $additional_filters
), dismissals as (
  select sum("wicket_sum") as dismissal_count from base_data 
)
SELECT
  sum("delivery_count")*1.0/(SELECT sum(dismissal_count) from dismissals) as strike_rate,
  sum("total_run_sum")*1.0/(SELECT sum(dismissal_count) from dismissals) as average,
  sum("total_run_sum")*6.0/(select count(*) from base_data) as economy
from base_data
""")

bowling_match_stats_template = Template("""
with base_data as (
  select * from delivery_records WHERE __time = TIME_PARSE('$dt') AND match_id='$match_id' AND "bowler_id" = '$bowler_id'
), overs_data as (
  select sum("total_run_sum") as runs_in_over from base_data group by "over"
)
SELECT
  sum("total_run_sum") as runs,
  count(*) as balls,
  (SELECT count(*) from base_data where "wicket_sum"=1) as wickets,
  (SELECT count(*) from overs_data where runs_in_over = 0) as maidens
from base_data
""")

def get_bowling_adversary_filter(batter_ids: Set[str]):
    return 'batter_id in (%s)'% ",".join(['\'%s\''%batter_id for batter_id in batter_ids])