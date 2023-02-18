from pyspark.sql import functions as f
from pyspark.sql.window import Window
from common_requirements import t20_df_with_boundaries
from path_manager import intermediate_data_t20_bowler_match_path

def aggregate_bowler_features():
    bowler_match_relevant_dimensions = ["match_id", "dt", "venue_name", "bowler_name", "bowler_id"] 
    bowler_over_relevant_dimensions = bowler_match_relevant_dimensions + ["over"]
    bowler_relevant_metrics = ["total_runs","is_wicket"]

    t20_bowler_df = t20_df_with_boundaries.select(bowler_over_relevant_dimensions+bowler_relevant_metrics)

    t20_bowler_over_df = t20_bowler_df.groupBy(bowler_over_relevant_dimensions) \
        .agg(
            f.sum("total_runs").alias("over_total_run_sum"),
            f.sum("is_wicket").alias("over_wicket_sum"),
            f.count("bowler_id").alias("over_deliveries")
        )\
        .withColumn('is_maiden', f.when(f.col('over_total_run_sum') == 0 , 1).otherwise(0))


    t20_bowler_match_df = t20_bowler_over_df.groupBy(bowler_match_relevant_dimensions) \
        .agg(
            f.sum("over_total_run_sum").alias("total_run_sum"),
            f.sum("over_wicket_sum").alias("wicket_sum"),
            f.sum("is_maiden").alias("maiden_count"),
            f.sum("over_deliveries").alias("deliveries"),
        )

    w_30d = (
        Window.partitionBy("bowler_id","bowler_name")
        .orderBy(f.unix_timestamp(f.col("dt").cast("timestamp")))
        .rangeBetween(-30*86400, Window.currentRow)
    )
    w_90d = (
        Window.partitionBy("bowler_id","bowler_name")
        .orderBy(f.unix_timestamp(f.col("dt").cast("timestamp")))
        .rangeBetween(-90*86400, Window.currentRow)
    )
    w_300d = (
        Window.partitionBy("bowler_id","bowler_name")
        .orderBy(f.unix_timestamp(f.col("dt").cast("timestamp")))
        .rangeBetween(-300*86400, Window.currentRow)
    )
    w_1000d = (
        Window.partitionBy("bowler_id","bowler_name")
        .orderBy(f.unix_timestamp(f.col("dt").cast("timestamp")))
        .rangeBetween(-1000*86400, Window.currentRow)
    )

    w_1000d_venue = (
        Window.partitionBy("bowler_id","bowler_name","venue_name")
        .orderBy(f.unix_timestamp(f.col("dt").cast("timestamp")))
        .rangeBetween(-1000*86400, Window.currentRow)
    )

    windowed_stats_df = t20_bowler_match_df.select(
        "dt","match_id","bowler_id","bowler_name","venue_name","total_run_sum", "deliveries", "wicket_sum", "maiden_count", 
        f.sum("total_run_sum").over(w_30d).alias("total_runs_30D"),
        f.sum("total_run_sum").over(w_90d).alias("total_runs_90D"),
        f.sum("total_run_sum").over(w_300d).alias("total_runs_300D"),
        f.sum("total_run_sum").over(w_1000d).alias("total_runs_1000D"),
        f.sum("total_run_sum").over(w_1000d_venue).alias("total_runs_1000D_venue"),
        
        f.sum("deliveries").over(w_30d).alias("deliveries_30D"),
        f.sum("deliveries").over(w_90d).alias("deliveries_90D"),
        f.sum("deliveries").over(w_300d).alias("deliveries_300D"),
        f.sum("deliveries").over(w_1000d).alias("deliveries_1000D"),
        f.sum("deliveries").over(w_1000d_venue).alias("deliveries_1000D_venue"),

        f.sum("wicket_sum").over(w_30d).alias("wicket_sum_30D"),
        f.sum("wicket_sum").over(w_90d).alias("wicket_sum_90D"),
        f.sum("wicket_sum").over(w_300d).alias("wicket_sum_300D"),
        f.sum("wicket_sum").over(w_1000d).alias("wicket_sum_1000D"),
        f.sum("wicket_sum").over(w_1000d_venue).alias("wicket_sum_1000D_venue"),
        
        f.sum("maiden_count").over(w_30d).alias("maiden_count_30D"),
        f.sum("maiden_count").over(w_90d).alias("maiden_count_90D"),
        f.sum("maiden_count").over(w_300d).alias("maiden_count_300D"),
        f.sum("maiden_count").over(w_1000d).alias("maiden_count_1000D"),
        f.sum("maiden_count").over(w_1000d_venue).alias("maiden_count_1000D_venue")  
    )

    windowed_stats_df_with_avg = windowed_stats_df\
        .withColumn("bowling_avg_30D", f.when(f.col("wicket_sum_30D") > 0, f.col("total_runs_30D")/f.col("wicket_sum_30D")).otherwise(f.col("total_runs_30D"))) \
        .withColumn("bowling_avg_90D", f.when(f.col("wicket_sum_90D") > 0, f.col("total_runs_90D")/f.col("wicket_sum_90D")).otherwise(f.col("total_runs_90D"))) \
        .withColumn("bowling_avg_300D", f.when(f.col("wicket_sum_300D") > 0, f.col("total_runs_300D")/f.col("wicket_sum_300D")).otherwise(f.col("total_runs_300D"))) \
        .withColumn("bowling_avg_1000D", f.when(f.col("wicket_sum_1000D") > 0, f.col("total_runs_1000D")/f.col("wicket_sum_1000D")).otherwise(f.col("total_runs_1000D"))) \
        .withColumn("bowling_avg_1000D_venue", f.when(f.col("wicket_sum_1000D_venue") > 0, f.col("total_runs_1000D_venue")/f.col("wicket_sum_1000D_venue")).otherwise(f.col("total_runs_1000D_venue")))

    windowed_stats_df_with_avg_sr = windowed_stats_df_with_avg\
        .withColumn("bowling_sr_30D", f.when(f.col("wicket_sum_30D") > 0, f.col("deliveries_30D")/f.col("wicket_sum_30D")).otherwise(f.col("deliveries_30D"))) \
        .withColumn("bowling_sr_90D", f.when(f.col("wicket_sum_90D") > 0, f.col("deliveries_90D")/f.col("wicket_sum_90D")).otherwise(f.col("deliveries_90D"))) \
        .withColumn("bowling_sr_300D", f.when(f.col("wicket_sum_300D") > 0, f.col("deliveries_300D")/f.col("wicket_sum_300D")).otherwise(f.col("deliveries_300D"))) \
        .withColumn("bowling_sr_1000D", f.when(f.col("wicket_sum_1000D") > 0, f.col("deliveries_1000D")/f.col("wicket_sum_1000D")).otherwise(f.col("deliveries_1000D"))) \
        .withColumn("bowling_sr_1000D_venue", f.when(f.col("wicket_sum_1000D_venue") > 0, f.col("deliveries_1000D_venue")/f.col("wicket_sum_1000D_venue")).otherwise(f.col("deliveries_1000D_venue")))

    windowed_stats_df_with_avg_sr_eco = windowed_stats_df_with_avg_sr\
        .withColumn("bowling_eco_30D", f.when(f.col("deliveries_30D") > 0, f.col("total_runs_30D") * 6.0/f.col("deliveries_30D")).otherwise(f.col("total_runs_30D") * 6.0)) \
        .withColumn("bowling_eco_90D", f.when(f.col("deliveries_90D") > 0, f.col("total_runs_90D") * 6.0/f.col("deliveries_90D")).otherwise(f.col("total_runs_90D") * 6.0)) \
        .withColumn("bowling_eco_300D", f.when(f.col("deliveries_300D") > 0, f.col("total_runs_300D") * 6.0/f.col("deliveries_300D")).otherwise(f.col("total_runs_300D") * 6.0)) \
        .withColumn("bowling_eco_1000D", f.when(f.col("deliveries_1000D") > 0, f.col("total_runs_1000D") * 6.0/f.col("deliveries_1000D")).otherwise(f.col("total_runs_1000D") * 6.0)) \
        .withColumn("bowling_eco_1000D_venue", f.when(f.col("deliveries_1000D_venue") > 0, f.col("total_runs_1000D_venue") * 6.0/f.col("deliveries_1000D_venue")).otherwise(f.col("total_runs_1000D_venue") * 6.0))

    windowed_stats_df_with_avg_sr_eco.write.format("parquet").partitionBy(["dt", "match_id"]).mode("overwrite").save(intermediate_data_t20_bowler_match_path)

