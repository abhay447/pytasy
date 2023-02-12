from string import Template

batting_stats_template = Template("""
with base_data as (
  select * from all_matches WHERE dt > '$start_date' AND dt < '$end_date'  AND match_type = 'T20' $additional_filters
), dismissals as (
  select sum(is_wicket) as dismissal_count from base_data WHERE wicket_player_id = '$batter_id'
)
SELECT
  sum(batter_runs)*100.0/count(*) as strike_rate,
  sum(batter_runs)*1.0/(SELECT sum(dismissal_count) + 1.0 from dismissals) as average
from base_data
where batter_id = '$batter_id'
""")

batting_match_stats_template = Template("""
with base_data as (
  select * from all_matches WHERE dt = '$dt' AND match_id='$match_id'
), batting_data as(
  select * from base_data where batter_id = '$batter_id'
), dismissals as (
  select sum(is_wicket) as dismissal_count from base_data WHERE wicket_player_id = '$batter_id'
)
SELECT
  sum(batter_runs) as runs,
  count(*) as balls,
  (SELECT sum(dismissal_count) + 1.0 from dismissals) as is_out,
  (SELECT count(*) from batting_data where batter_runs >=4 and batter_runs < 6) as boundaries_count,
  (SELECT count(*) from batting_data where batter_runs > 6) as sixes_count
from batting_data
""")



bowling_stats_template = Template("""
with base_data as (
  select * from all_matches WHERE dt > '$start_date' AND dt < '$end_date' AND bowler_id = '$bowler_id' AND match_type = 'T20' $additional_filters
), dismissals as (
  select sum(is_wicket) as dismissal_count from base_data 
)
SELECT
  count(*)*1.0/(SELECT sum(dismissal_count) + 1.0 from dismissals) as strike_rate,
  sum(total_runs)*1.0/(SELECT sum(dismissal_count) + 1.0 from dismissals) as average,
  sum(total_runs)*6.0/(select count(*) from base_data) as economy
from base_data
""")

bowling_match_stats_template = Template("""
with base_data as (
  select * from all_matches WHERE dt = '$dt' AND match_id='$match_id' AND bowler_id = '$bowler_id'
), overs_data as (
  select sum(total_runs) as runs_in_over from base_data group by over
)
SELECT
  sum(total_runs) as runs,
  count(*) as balls,
  (SELECT count(*) from base_data where is_wicket=1) as wickets,
  (SELECT count(*) from overs_data where runs_in_over = 0) as maidens
from base_data
""")



distinct_opposing_bowler_template = Template("""
  select distinct bowler_id from all_matches 
    WHERE dt = '$dt' 
    AND match_id = '$match_id'
    AND bowler_team != '$batter_team'
""")

distinct_opposing_batter_template = Template("""
  select distinct batter_id from all_matches 
    WHERE dt = '$dt' 
    AND match_id = '$match_id'
    AND batter_team != '$bowler_team'
""")

fielding_stats_template = Template("""
with base_data as (
  select * from t20_fielder_match_stats WHERE dt > '$start_date' AND dt < '$end_date'  AND match_type = 'T20' $additional_filters
)
SELECT sum(wicket_sum) as fielding_dismissals from base_data where wicket_fielder_id= '$fielder_id'
""")

fielding_match_stats_template = Template("""
with base_data as (
  select * from t20_fielder_match_stats WHERE dt = '$dt' AND match_id='$match_id'
)
SELECT sum(wicket_sum) as fielding_dismissals from base_data where wicket_fielder_id= '$fielder_id'
""")
  