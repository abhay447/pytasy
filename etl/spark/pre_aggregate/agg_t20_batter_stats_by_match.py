from pyspark.sql import functions as f
from pyspark.sql.window import Window
from common_requirements import t20_df_with_boundaries

output_path = '/home/abhay/work/dream11/processed_output/t20_batter_match_stats'

batter_relevant_dimensions = ["match_id", "dt", "venue_name", "batter_name", "batter_id"] 
batter_relevant_metrics = ["batter_runs","is_dismissed", "is_boundary", "is_six"]
t20_batter_delivery_df = t20_df_with_boundaries.select(batter_relevant_dimensions+batter_relevant_metrics)

t20_batter_match_df  = t20_batter_delivery_df.groupBy(batter_relevant_dimensions) \
    .agg(
        f.sum("batter_runs").alias("batter_run_sum"),
        f.sum("is_dismissed").alias("dismissals"),
        f.sum("is_boundary").alias("boundary_count"),
        f.sum("is_six").alias("six_count"),
        f.count("batter_id").alias("balls_faced")
    )

w_30d = (
    Window.partitionBy("batter_id","batter_name")
    .orderBy(f.unix_timestamp(f.col("dt").cast("timestamp")))
    .rangeBetween(-30*86400, Window.currentRow)
)
w_90d = (
    Window.partitionBy("batter_id","batter_name")
    .orderBy(f.unix_timestamp(f.col("dt").cast("timestamp")))
    .rangeBetween(-90*86400, Window.currentRow)
)
w_300d = (
    Window.partitionBy("batter_id","batter_name")
    .orderBy(f.unix_timestamp(f.col("dt").cast("timestamp")))
    .rangeBetween(-300*86400, Window.currentRow)
)
w_1000d = (
    Window.partitionBy("batter_id","batter_name")
    .orderBy(f.unix_timestamp(f.col("dt").cast("timestamp")))
    .rangeBetween(-1000*86400, Window.currentRow)
)

w_1000d_venue = (
    Window.partitionBy("batter_id","batter_name","venue_name")
    .orderBy(f.unix_timestamp(f.col("dt").cast("timestamp")))
    .rangeBetween(-1000*86400, Window.currentRow)
)


windowed_stats_df = t20_batter_match_df.select(
    "dt","match_id","batter_id","batter_name","venue_name","batter_run_sum", "balls_faced", "dismissals", "boundary_count", "six_count", 
    f.sum("batter_run_sum").over(w_30d).alias("batter_runs_30D"),
    f.sum("batter_run_sum").over(w_90d).alias("batter_runs_90D"),
    f.sum("batter_run_sum").over(w_300d).alias("batter_runs_300D"),
    f.sum("batter_run_sum").over(w_1000d).alias("batter_runs_1000D"),
    f.sum("batter_run_sum").over(w_1000d_venue).alias("batter_runs_1000D_venue"),
    
    f.sum("balls_faced").over(w_30d).alias("balls_faced_30D"),
    f.sum("balls_faced").over(w_90d).alias("balls_faced_90D"),
    f.sum("balls_faced").over(w_300d).alias("balls_faced_300D"),
    f.sum("balls_faced").over(w_1000d).alias("balls_faced_1000D"),
    f.sum("balls_faced").over(w_1000d_venue).alias("balls_faced_1000D_venue"),

    f.sum("dismissals").over(w_30d).alias("dismissals_30D"),
    f.sum("dismissals").over(w_90d).alias("dismissals_90D"),
    f.sum("dismissals").over(w_300d).alias("dismissals_300D"),
    f.sum("dismissals").over(w_1000d).alias("dismissals_1000D"),
    f.sum("dismissals").over(w_1000d_venue).alias("dismissals_1000D_venue"),
    
    f.sum("boundary_count").over(w_30d).alias("boundary_count_30D"),
    f.sum("boundary_count").over(w_90d).alias("boundary_count_90D"),
    f.sum("boundary_count").over(w_300d).alias("boundary_count_300D"),
    f.sum("boundary_count").over(w_1000d).alias("boundary_count_1000D"),
    f.sum("boundary_count").over(w_1000d_venue).alias("boundary_count_1000D_venue"),
    
    f.sum("six_count").over(w_30d).alias("six_count_30D"),
    f.sum("six_count").over(w_90d).alias("six_count_90D"),
    f.sum("six_count").over(w_300d).alias("six_count_300D"),
    f.sum("six_count").over(w_1000d).alias("six_count_1000D"),
    f.sum("six_count").over(w_1000d_venue).alias("six_count_1000D_venue"),    
)

windowed_stats_df_with_avg = windowed_stats_df\
    .withColumn("batting_avg_30D", f.when(f.col("dismissals_30D") > 0, f.col("batter_runs_30D")/f.col("dismissals_30D")).otherwise(f.col("batter_runs_30D"))) \
    .withColumn("batting_avg_90D", f.when(f.col("dismissals_90D") > 0, f.col("batter_runs_90D")/f.col("dismissals_90D")).otherwise(f.col("batter_runs_90D"))) \
    .withColumn("batting_avg_300D", f.when(f.col("dismissals_300D") > 0, f.col("batter_runs_300D")/f.col("dismissals_300D")).otherwise(f.col("batter_runs_300D"))) \
    .withColumn("batting_avg_1000D", f.when(f.col("dismissals_1000D") > 0, f.col("batter_runs_1000D")/f.col("dismissals_1000D")).otherwise(f.col("batter_runs_1000D"))) \
    .withColumn("batting_avg_1000D_venue", f.when(f.col("dismissals_1000D_venue") > 0, f.col("batter_runs_1000D_venue")/f.col("dismissals_1000D_venue")).otherwise(f.col("batter_runs_1000D_venue")))

windowed_stats_df_with_avg_sr = windowed_stats_df_with_avg\
    .withColumn("batting_sr_30D", f.when(f.col("balls_faced_30D") > 0, f.col("batter_runs_30D")/f.col("balls_faced_30D")).otherwise(f.col("batter_runs_30D"))) \
    .withColumn("batting_sr_90D", f.when(f.col("balls_faced_90D") > 0, f.col("batter_runs_90D")/f.col("balls_faced_90D")).otherwise(f.col("batter_runs_90D"))) \
    .withColumn("batting_sr_300D", f.when(f.col("balls_faced_300D") > 0, f.col("batter_runs_300D")/f.col("balls_faced_300D")).otherwise(f.col("batter_runs_300D"))) \
    .withColumn("batting_sr_1000D", f.when(f.col("balls_faced_1000D") > 0, f.col("batter_runs_1000D")/f.col("balls_faced_1000D")).otherwise(f.col("batter_runs_1000D"))) \
    .withColumn("batting_sr_1000D_venue", f.when(f.col("balls_faced_1000D_venue") > 0, f.col("batter_runs_1000D_venue")/f.col("balls_faced_1000D_venue")).otherwise(f.col("batter_runs_1000D_venue")))

windowed_stats_df_with_avg_sr.write.format("parquet").partitionBy(["dt", "match_id"]).mode("overwrite").save(output_path)

