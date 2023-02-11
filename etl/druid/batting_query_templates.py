from string import Template
from typing import Set

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

batting_match_stats_template = Template("""
with base_data as (
  select * from delivery_records WHERE __time = TIME_PARSE('$dt') AND match_id='$match_id'
), batting_data as(
  select * from base_data where "batter_id" = '$batter_id'
), dismissals as (
  select sum("wicket_sum") as dismissal_count from base_data WHERE "wicket_player_id" = '$batter_id'
)
SELECT
  sum("batter_run_sum") as runs,
  count(*) as balls,
  (SELECT sum(dismissal_count) from dismissals) as is_out,
  (SELECT count(*) from batting_data where "batter_run_sum" >=4 and "batter_run_sum" < 6) as boundaries_count,
  (SELECT count(*) from batting_data where "batter_run_sum" > 6) as sixes_count
from batting_data
""")

def get_batting_adversary_filter(bowler_ids: Set[str]):
    return 'bowler_id in (%s)'% ",".join(['\'%s\''%bowler_id for bowler_id in bowler_ids])