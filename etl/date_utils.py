from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def get_min_max_date(df: DataFrame):
    return df \
        .agg(F.max(df.start_date) \
        .alias('max_start_date'), F.min(df.start_date).alias('min_start_date')) \
        .collect()[0]