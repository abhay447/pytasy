from pyspark.sql.functions import sum, count
from etl.spark.pre_aggregate.common_requirements import t20_df_with_boundaries

output_path = '/home/abhay/work/dream11/processed_output/t20_batter_match_stats'

batter_relevant_dimensions = ["match_id", "dt", "venue_name", "batter_name", "batter_id"] 
batter_relevant_metrics = ["batter_runs","is_dismissed", "is_boundary", "is_six"]
t20_batter_delivery_df = t20_df_with_boundaries.select(batter_relevant_dimensions+batter_relevant_metrics)

t20_batter_match_df  = t20_batter_delivery_df.groupBy(batter_relevant_dimensions) \
    .agg(
        sum("batter_runs").alias("batter_run_sum"),
        sum("is_dismissed").alias("dismissals"),
        sum("is_boundary").alias("boundary_count"),
        sum("is_six").alias("six_count"),
        count("batter_id").alias("deliveries")
    )

t20_batter_match_df.write.format("parquet").partitionBy(["dt", "match_id"]).mode("overwrite").save(output_path)

