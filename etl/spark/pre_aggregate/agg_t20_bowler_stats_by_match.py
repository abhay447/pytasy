from pyspark.sql.functions import sum, count, when, col
from common_requirements import t20_df_with_boundaries

output_path = '/home/abhay/work/dream11/processed_output/t20_bowler_match_stats'

bowler_match_relevant_dimensions = ["match_id", "dt", "venue_name", "bowler_name", "bowler_id"] 
bowler_over_relevant_dimensions = bowler_match_relevant_dimensions + ["over"]
bowler_relevant_metrics = ["total_runs","is_wicket"]

t20_bowler_df = t20_df_with_boundaries.select(bowler_over_relevant_dimensions+bowler_relevant_metrics)

t20_bowler_over_df = t20_bowler_df.groupBy(bowler_over_relevant_dimensions) \
    .agg(
        sum("total_runs").alias("over_total_run_sum"),
        sum("is_wicket").alias("over_wicket_sum"),
        count("bowler_id").alias("over_deliveries")
    )\
    .withColumn('is_maiden', when(col('over_total_run_sum') == 0 , 1).otherwise(0))


t20_bowler_match_df = t20_bowler_over_df.groupBy(bowler_match_relevant_dimensions) \
    .agg(
        sum("over_total_run_sum").alias("total_run_sum"),
        sum("over_wicket_sum").alias("wicket_sum"),
        sum("is_maiden").alias("maiden_count"),
        sum("over_deliveries").alias("deliveries"),
    )

t20_bowler_match_df.write.format("parquet").partitionBy(["dt", "match_id"]).mode("overwrite").save(output_path)

