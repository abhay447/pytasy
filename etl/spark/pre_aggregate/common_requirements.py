from pyspark.sql import functions as f
from etl.spark.spark_session_helper import spark

parquet_input_path = '/home/abhay/work/dream11/processed_output/delivery_parquet'

t20_df_with_boundaries = spark.read.parquet(parquet_input_path)\
    .where(f.col('match_type') == 'T20') \
    .where(f.col('dt') > '2015-12-31') \
    .withColumn('is_boundary', f.when((f.col('batter_runs') >= 4) & (f.col("batter_runs") < 6), 1).otherwise(0)) \
    .withColumn('is_six', f.when(f.col("batter_runs") >= 6, 1).otherwise(0)) \
    .withColumn('is_dismissed', f.when(f.col("wicket_player_id") == f.col("batter_id"), 1).otherwise(0))