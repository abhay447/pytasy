from typing import List
from pyspark import Row
from pyspark.sql import SparkSession, DataFrame
from etl.commons.stats_helper import map_player_match_for_training, sequential_player_match_for_training
from etl.commons.training_record_schema import training_record_schema
from etl.datawriter import write_dataframe

output_path = '/home/abhay/work/dream11/processed_output/training_rows'
use_sequential_spark_code = True

spark = SparkSession.builder.appName('SparkByExamples.com').config('spark.driver.bindAddress','localhost').config("spark.ui.port","4050").config("spark.driver.memory","6g").getOrCreate()
spark.read.parquet("processed_output/delivery_parquet").cache().createOrReplaceTempView("all_matches")

train_t20_match_id_query = """
    with training_collection as (
        select distinct match_id from all_matches where dt>='2018-01-01' and match_type='T20' limit 15
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
t20_match_ids = spark.sql(train_t20_match_id_query)

if not use_sequential_spark_code:
    rows_with_features_rdd = t20_match_ids.rdd.map(map_player_match_for_training)
    features_df: DataFrame = spark.createDataFrame(rows_with_features_rdd, schema=training_record_schema).coalesce(20)
else:
    match_player_id_rows = t20_match_ids.collect()
    rows_with_features_list: List[Row] = []
    for i in range(len(match_player_id_rows)):
        rows_with_features_list.append(sequential_player_match_for_training(match_player_id_rows[i], spark))
        print("finished row %d out %d"%(i, len(match_player_id_rows)))
    features_df: DataFrame = spark.createDataFrame(rows_with_features_list, schema=training_record_schema).coalesce(20)

write_dataframe(df=features_df,output_path=output_path, overwrite=True,spark=spark)
