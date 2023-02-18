from etl.spark.spark_session_helper import spark
from pyspark.sql import functions as f
from etl.commons.stats_helper import get_fantasy_points_udf
from path_manager import intermediate_data_all_train_rows_path, intermediate_data_t20_batter_match_path, intermediate_data_t20_bowler_match_path, intermediate_data_t20_fielder_match_path
from path_manager import model_test_input_path, model_train_input_path
from path_manager import raw_data_flatten_path

from etl.spark.pre_aggregate import agg_t20_batter_stats_by_match, agg_t20_bowler_stats_by_match, agg_t20_fielder_stats_by_match

def aggregate_player_stats():
    t20_df_with_boundaries = spark.read.parquet(raw_data_flatten_path)\
        .where(f.col('match_type') == 'T20') \
        .where(f.col('dt') > '2015-12-31') \
        .withColumn('is_boundary', f.when((f.col('batter_runs') >= 4) & (f.col("batter_runs") < 6), 1).otherwise(0)) \
        .withColumn('is_six', f.when(f.col("batter_runs") >= 6, 1).otherwise(0)) \
        .withColumn('is_dismissed', f.when(f.col("wicket_player_id") == f.col("batter_id"), 1).otherwise(0)) \
        .withColumn('is_male', f.when(f.col("gender") == "male", 1).otherwise(0))
    print("*******************************************************************************")
    print("Agrgegating fielder stats")
    agg_t20_fielder_stats_by_match.aggregate_fielder_stats(t20_df_with_boundaries)
    print("*******************************************************************************")
    print("Agrgegating batter stats")
    agg_t20_batter_stats_by_match.aggregate_batter_features(t20_df_with_boundaries)
    print("*******************************************************************************")
    print("Agrgegating bowler stats")
    agg_t20_bowler_stats_by_match.aggregate_bowler_features(t20_df_with_boundaries)
    print("*******************************************************************************")
    print("combining all stats into raw feature rows")

    t20_bowler_match_stats_df = spark.read.parquet(intermediate_data_t20_bowler_match_path)\
        .withColumnRenamed("match_id", "bowler_match_id")\
        .withColumnRenamed("dt", "bowler_dt")\
        .withColumnRenamed("venue_name", "bowler_venue_name")\
        .withColumnRenamed("is_male", "bowler_is_male")
    t20_batter_match_stats_df = spark.read.parquet(intermediate_data_t20_batter_match_path)\
        .withColumnRenamed("match_id", "batter_match_id")\
        .withColumnRenamed("dt", "batter_dt")\
        .withColumnRenamed("venue_name", "batter_venue_name")\
        .withColumnRenamed("is_male", "batter_is_male")

    t20_fielder_match_stats_df = spark.read.parquet(intermediate_data_t20_fielder_match_path)\
        .withColumnRenamed("match_id", "fielder_match_id")\
        .withColumnRenamed("dt", "fielder_dt")\
        .withColumnRenamed("is_male", "fielder_is_male")

    bat_bowl_df = t20_batter_match_stats_df \
        .join(t20_bowler_match_stats_df, 
            [
                t20_batter_match_stats_df.batter_id == t20_bowler_match_stats_df.bowler_id,
                t20_batter_match_stats_df.batter_match_id == t20_bowler_match_stats_df.bowler_match_id,
            ],
            how="full_outer"
        )\
        .withColumn("bat_bowl_player_id", f.coalesce(t20_batter_match_stats_df.batter_id,t20_bowler_match_stats_df.bowler_id))\
        .withColumn("bat_bowl_dt", f.coalesce(t20_batter_match_stats_df.batter_dt,t20_bowler_match_stats_df.bowler_dt))\
        .withColumn("bat_bowl_match_id", f.coalesce(t20_batter_match_stats_df.batter_match_id,t20_bowler_match_stats_df.bowler_match_id))\
        .withColumn("venue_name", f.coalesce(t20_batter_match_stats_df.batter_venue_name,t20_bowler_match_stats_df.bowler_venue_name))\
        .withColumn("bat_bowl_is_male", f.coalesce(t20_batter_match_stats_df.batter_is_male,t20_bowler_match_stats_df.bowler_is_male))\
        .drop(
            "bowler_match_id","bowler_dt","bowler_venue_name","batter_match_id",
            "batter_dt","batter_venue_name", "batter_id", "bowler_id", "bowler_is_male", "batter_is_male")

    bat_bowl_field_df = bat_bowl_df \
        .join(t20_fielder_match_stats_df, 
            [
                bat_bowl_df.bat_bowl_player_id == t20_fielder_match_stats_df.wicket_fielder_id,
                bat_bowl_df.bat_bowl_match_id == t20_fielder_match_stats_df.fielder_match_id,
            ],
            how="full_outer"
        )\
        .withColumn("player_id", f.coalesce(bat_bowl_df.bat_bowl_player_id,t20_fielder_match_stats_df.wicket_fielder_id))\
        .withColumn("dt", f.coalesce(bat_bowl_df.bat_bowl_dt,t20_fielder_match_stats_df.fielder_dt))\
        .withColumn("match_id", f.coalesce(bat_bowl_df.bat_bowl_match_id,t20_fielder_match_stats_df.fielder_match_id))\
        .withColumn("is_male", f.coalesce(bat_bowl_df.bat_bowl_is_male,t20_fielder_match_stats_df.fielder_is_male))\
        .drop(
            "fielder_match_id","fielder_dt","wicket_fielder_id","bat_bowl_player_id",
            "bat_bowl_dt", "bat_bowl_match_id","bat_bowl_is_male", "fielder_is_male")

    bat_bowl_field_df_with_points = bat_bowl_field_df\
        .withColumn(
            "fantasy_points",get_fantasy_points_udf(
                bat_bowl_field_df.batter_run_sum, bat_bowl_field_df.dismissals, bat_bowl_field_df.balls_faced,
                bat_bowl_field_df.boundary_count, bat_bowl_field_df.six_count,
                bat_bowl_field_df.total_run_sum, bat_bowl_field_df.wicket_sum, bat_bowl_field_df.deliveries, bat_bowl_field_df.maiden_count,
                bat_bowl_field_df.fielding_wicket_sum
            )
        ).na.fill(0)
    return bat_bowl_field_df_with_points

def prepare_model_feature_data():
    bat_bowl_field_df_with_points = aggregate_player_stats()
    print("*******************************************************************************")
    print("saving all feature rows df")
    bat_bowl_field_df_with_points.write.format("parquet").partitionBy(["dt", "match_id"]).mode("overwrite").save(intermediate_data_all_train_rows_path)

    print("*******************************************************************************")
    print("split all feature rows df into test and train splits")
    splits = bat_bowl_field_df_with_points.randomSplit([0.7, 0.3], 42)
    train_input_df = splits[0]
    test_input_df = splits[0]

    print("*******************************************************************************")
    print("saving training rows df")
    train_input_df.write.format("parquet").mode("overwrite").save(model_train_input_path)
    print("*******************************************************************************")
    print("saving test rows df")
    print("saving training rows df")
    test_input_df.write.format("parquet").mode("overwrite").save(model_test_input_path)