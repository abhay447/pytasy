from pyspark.sql.types import StructType, StructField, StringType, DoubleType

training_record_schema = StructType([
    # StructField("feature_batting_sr_30_D",DoubleType(), True),
    # StructField("feature_batting_avg_30_D",DoubleType(), True),
    StructField("feature_batting_sr_90_D",DoubleType(), True),
    StructField("feature_batting_avg_90_D",DoubleType(), True),
    # StructField("feature_batting_sr_180_D",DoubleType(), True),
    # StructField("feature_batting_avg_180_D",DoubleType(), True),
    StructField("feature_batting_sr_1800_D",DoubleType(), True),
    StructField("feature_batting_avg_1800_D",DoubleType(), True),
    StructField("feature_batting_sr_1800_D_venue",DoubleType(), True),
    StructField("feature_batting_avg_1800_D_venue",DoubleType(), True),
    # StructField("feature_batting_sr_1800_D_versus",DoubleType(), True),
    # StructField("feature_batting_avg_1800_D_versus",DoubleType(), True),
    # StructField("feature_batting_sr_1800_D_venue_versus",DoubleType(), True),
    # StructField("feature_batting_avg_1800_D_venue_versus",DoubleType(), True),

    # StructField("feature_bowling_sr_30_D",DoubleType(), True),
    # StructField("feature_bowling_avg_30_D",DoubleType(), True),
    # StructField("feature_bowling_economy_30_D",DoubleType(), True),
    StructField("feature_bowling_sr_90_D",DoubleType(), True),
    StructField("feature_bowling_avg_90_D",DoubleType(), True),
    StructField("feature_bowling_economy_90_D",DoubleType(), True),
    # StructField("feature_bowling_sr_180_D",DoubleType(), True),
    # StructField("feature_bowling_avg_180_D",DoubleType(), True),
    # StructField("feature_bowling_economy_180_D",DoubleType(), True),
    StructField("feature_bowling_sr_1800_D",DoubleType(), True),
    StructField("feature_bowling_avg_1800_D",DoubleType(), True),
    StructField("feature_bowling_economy_1800_D",DoubleType(), True),
    StructField("feature_bowling_sr_1800_D_venue",DoubleType(), True),
    StructField("feature_bowling_avg_1800_D_venue",DoubleType(), True),
    StructField("feature_bowling_economy_1800_D_venue",DoubleType(), True),
    # StructField("feature_bowling_sr_1800_D_versus",DoubleType(), True),
    # StructField("feature_bowling_avg_1800_D_versus",DoubleType(), True),
    # StructField("feature_bowling_economy_1800_D_versus",DoubleType(), True),
    # StructField("feature_bowling_sr_1800_D_venue_versus",DoubleType(), True),
    # StructField("feature_bowling_avg_1800_D_venue_versus",DoubleType(), True),
    # StructField("feature_bowling_economy_1800_D_venue_versus",DoubleType(), True),
    StructField("feature_fielding_dismissals_1800_D",DoubleType(), True),
    StructField("player_name",StringType(), True),
    StructField("player_id",StringType(), True),
    StructField("dt",StringType(), True),
    StructField("venue",StringType(), True),
    StructField("team",StringType(), True),
    StructField("fantasy_points",DoubleType(), True),
])