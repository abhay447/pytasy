from pyspark.sql.functions import sum, count, trunc
from common_requirements import t20_df_with_boundaries

output_path = '/home/abhay/work/dream11/processed_output/t20_batter_bowler_month_stats'

batter_bowler_relevant_dimensions = ["dt", "venue_name","batter_name", "batter_id", "bowler_name", "bowler_id"] 
batter_bowler_relevant_metrics = ["batter_runs","total_runs","is_dismissed"]

t20_batter_bowler_df = t20_df_with_boundaries\
    .select(batter_bowler_relevant_dimensions+batter_bowler_relevant_metrics) \
    .withColumn("dt", trunc("dt", "month"))

t20_batter_bowler_df_per_month = t20_batter_bowler_df.groupBy(batter_bowler_relevant_dimensions) \
    .agg(
        sum("batter_runs").alias("batter_run_sum"),
        sum("total_runs").alias("total_run_sum"),
        count("bowler_id").alias("deliveries"),
        sum("is_dismissed").alias("wicket_sum")
    )

t20_batter_bowler_df_per_month.write.format("parquet").partitionBy(["dt"]).mode("overwrite").save(output_path)

