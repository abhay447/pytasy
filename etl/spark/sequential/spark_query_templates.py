from string import Template

batting_stats_template = Template("""
with base_data as (
  select * from t20_batter_match_stats WHERE dt > '$start_date' AND dt < '$end_date' and batter_id = '$batter_id' $additional_filters
)
SELECT
  sum(batter_run_sum)*100.0/sum(deliveries) as strike_rate,
  sum(batter_run_sum)*1.0/(sum(dismissals) + 1.0) as average
from base_data
where batter_id = '$batter_id'
""")

batting_match_stats_template = Template("""
with base_data as (
  select * from t20_batter_match_stats WHERE dt = '$dt' AND match_id='$match_id' and batter_id = '$batter_id'
)
SELECT
  batter_run_sum as runs,
  deliveries as balls,
  dismissals as dismissals,
  boundary_count as boundaries_count,
  six_count as sixes_count
from base_data
""")



bowling_stats_template = Template("""
with base_data as (
  select * from t20_bowler_match_stats WHERE dt > '$start_date' AND dt < '$end_date' AND bowler_id = '$bowler_id'  $additional_filters
)
SELECT
  sum(deliveries)/(sum(wicket_sum) + 1.0) as strike_rate,
  sum(total_run_sum)*1.0/(sum(wicket_sum) + 1.0) as average,
  sum(total_run_sum)*6.0/sum(deliveries) as economy
from base_data
""")

bowling_match_stats_template = Template("""
with base_data as (
  select * from t20_bowler_match_stats WHERE dt = '$dt' AND match_id='$match_id' AND bowler_id = '$bowler_id'
)
SELECT
  total_run_sum as runs,
  deliveries as balls,
  wicket_sum as wickets,
  maiden_count as maidens
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
  select * from t20_fielder_match_stats WHERE dt > '$start_date' AND dt < '$end_date' $additional_filters
)
SELECT sum(wicket_sum) as fielding_dismissals from base_data where wicket_fielder_id= '$fielder_id'
""")

fielding_match_stats_template = Template("""
with base_data as (
  select * from t20_fielder_match_stats WHERE dt = '$dt' AND match_id='$match_id'
)
SELECT sum(wicket_sum) as fielding_dismissals from base_data where wicket_fielder_id= '$fielder_id'
""")
  