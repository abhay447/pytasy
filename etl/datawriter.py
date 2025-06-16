from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from etl.datareader import read_delivery_records_as_dataframe
from etl.date_utils import get_min_max_date
from pyspark.sql.functions import col
import time

def write_bulk_historical_data(raw_data_prefix: str, output_path: str, spark: SparkSession) -> None:
    spark_rows_df = read_delivery_records_as_dataframe(raw_data_prefix,spark)
    write_dataframe_with_date_partition(df=spark_rows_df, output_path=output_path,overwrite=True, spark=spark)

def merge_new_data(output_path: str, historical_data_prefix:str, new_data_prefix: str,  spark: SparkSession) -> None:
    historical_dataset: DataFrame = spark.read.parquet(output_path)
    new_data_df = read_delivery_records_as_dataframe(new_data_prefix,spark)
    new_data_date_range = get_min_max_date(new_data_df)
    overlapping_historical_data = historical_dataset\
        .filter((historical_dataset.start_date>= new_data_date_range.min_start_date) & (historical_dataset.start_date<= new_data_date_range.max_start_date))
    overlapping_historical_data.registerTempTable("matches")
    existing_match_ids = set([row.match_id for row in spark.sql("Select distinct match_id from matches").collect()])
    new_matches_df = new_data_df.filter(new_data_df.match_id.isin(existing_match_ids) == False)
    write_dataframe_with_date_partition(df=new_matches_df, output_path=output_path, overwrite=False, spark=spark)

def write_dataframe_with_date_partition(df: DataFrame, output_path: str, overwrite: bool,  spark: SparkSession):
    start_time = time.time()
    write_mode = "overwrite" if overwrite else "append"
    df.withColumn("dt", col("start_date")).write.format("parquet").partitionBy("dt").mode(write_mode).save(output_path)
    print("finished dataframe write to path %s in %s seconds ---" % (output_path, time.time() - start_time))

def write_dataframe(df: DataFrame, output_path: str, overwrite: bool,  spark: SparkSession):
    write_mode = "overwrite" if overwrite else "append"
    df.write.format("parquet").mode(write_mode).save(output_path)
