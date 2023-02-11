from string import Template

fielding_stats_template = Template("""
with base_data as (
  select * from delivery_records WHERE __time > '$start_date' AND __time < '$end_date'  AND "match_type" = 'T20' $additional_filters
)
SELECT sum(wicket_sum) as fielding_dismissals from base_data where "wicket_fielder_id"= '$fielder_id'
""")

fielding_match_stats_template = Template("""
with base_data as (
  select * from delivery_records WHERE __time = TIME_PARSE('$dt') AND match_id='$match_id'
)
SELECT sum(wicket_sum) as fielding_dismissals from base_data where "wicket_fielder_id"= '$fielder_id'
""")