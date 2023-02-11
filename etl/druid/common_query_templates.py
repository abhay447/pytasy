from string import Template

distinct_opposing_bowler_template = Template("""
  select distinct bowler_id from delivery_records 
    WHERE __time  = TIME_PARSE('$dt') 
    AND match_id = '$match_id'
    AND bowler_team != '$batter_team'
""")

distinct_opposing_batter_template = Template("""
  select distinct batter_id from delivery_records 
    WHERE __time  = TIME_PARSE('$dt') 
    AND match_id = '$match_id'
    AND batter_team != '$bowler_team'
""")

def get_venue_filter(venue_name:str):
    return 'venue_name=\'%s\''%venue_name