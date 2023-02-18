from pyspark.sql import functions as f
from pyspark.sql.window import Window
from common_requirements import t20_df_with_boundaries
from path_manager import intermediate_data_t20_fielder_match_path

def aggregate_fielder_stats():
    fielder_relevant_dimensions = ["dt","match_id","wicket_fielder_id", "wicket_fielder_name"] 
    fielder_relevant_metrics = ["is_dismissed"]

    t20_fielder_df = t20_df_with_boundaries\
        .select(fielder_relevant_dimensions+fielder_relevant_metrics) \
        .filter(f.col("wicket_fielder_id").isNotNull())

    t20_fielder_df_per_month = t20_fielder_df.groupBy(fielder_relevant_dimensions) \
        .agg(
            f.sum("is_dismissed").alias("fielding_wicket_sum")
        )

    w_300d = (
        Window.partitionBy("wicket_fielder_id","wicket_fielder_name")
        .orderBy(f.unix_timestamp(f.col("dt").cast("timestamp")))
        .rangeBetween(-300*86400, Window.currentRow)
    )
    w_1000d = (
        Window.partitionBy("wicket_fielder_id","wicket_fielder_name")
        .orderBy(f.unix_timestamp(f.col("dt").cast("timestamp")))
        .rangeBetween(-1000*86400, Window.currentRow)
    )

    windowed_stats_df = t20_fielder_df_per_month.select(
        "dt","match_id","wicket_fielder_id","wicket_fielder_name","fielding_wicket_sum",
        f.sum("fielding_wicket_sum").over(w_300d).alias("fielding_wicket_sum_300D"),
        f.sum("fielding_wicket_sum").over(w_1000d).alias("fielding_wicket_sum_1000D")
    )

    windowed_stats_df.write.format("parquet").partitionBy(["dt","match_id"]).mode("overwrite").save(intermediate_data_t20_fielder_match_path)