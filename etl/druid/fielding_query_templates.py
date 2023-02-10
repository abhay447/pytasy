from string import Template

fielding_stats_template = Template("""
with base_data as (
  select * from delivery_records WHERE __time > '$start_date' AND __time < '$end_date'  AND "match_type" = 'T20' $additional_filters
)
SELECT sum(wicket_sum) as fielding_dismissals, count(distinct match_id) as match_count from base_data where "wicket_fielder_id"= '$fielder_id'
""")