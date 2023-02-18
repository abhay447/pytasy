from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from etl.spark.spark_session_helper import spark
from path_manager import raw_data_flatten_path

t20_df_with_boundaries: DataFrame = spark.read.parquet(raw_data_flatten_path)\
    .where(f.col('match_type') == 'T20') \
    .where(f.col('dt') > '2015-12-31') \
    .withColumn('is_boundary', f.when((f.col('batter_runs') >= 4) & (f.col("batter_runs") < 6), 1).otherwise(0)) \
    .withColumn('is_six', f.when(f.col("batter_runs") >= 6, 1).otherwise(0)) \
    .withColumn('is_dismissed', f.when(f.col("wicket_player_id") == f.col("batter_id"), 1).otherwise(0))