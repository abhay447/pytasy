from pyspark.sql import SparkSession, Row
from pyspark.rdd import RDD
from etl.druid.stats_helper import prepare_player_match_for_training

spark = SparkSession.builder.appName('SparkByExamples.com').config('spark.driver.bindAddress','localhost').config("spark.ui.port","4050").getOrCreate()
spark.read.parquet("processed_output/delivery_parquet").registerTempTable("all_matches")

train_t20_match_id_query = """
    with training_collection as (
        select distinct match_id from all_matches where dt>='2018-01-01' and match_type='T20' limit 1500
    ), base_data as (
        select * from all_matches where dt>='2018-01-01' and match_id in (select match_id from training_collection)
    ), player_ids as (
        select distinct match_id, dt, venue_name, batter_team as team, batter_id as player_id, batter_name as player_name from base_data where batter_id is not null
        union
        select distinct match_id, dt, venue_name, bowler_team as team, bowler_id as player_id, bowler_name as player_name from base_data where bowler_id is not null
        union
        select distinct match_id, dt, venue_name, batter_team as team, wicket_player_id as player_id, wicket_player_name as player_name from base_data where wicket_player_id is not null
        union
        select distinct match_id, dt, venue_name, bowler_team as team, wicket_fielder_id as player_id, wicket_fielder_name as player_name from base_data where wicket_fielder_id is not null
    )
    select distinct match_id, dt, venue_name, team, player_id, player_name from player_ids 
"""
train_t20_match_ids = spark.sql(train_t20_match_id_query)

training_rows: RDD[Row] = train_t20_match_ids.rdd.flatMap(prepare_player_match_for_training)