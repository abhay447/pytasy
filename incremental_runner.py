from etl.datawriter import merge_new_data
from pyspark.sql import SparkSession
from path_manager import raw_data_dowload_path, raw_30_days_data_dowload_path, raw_data_flatten_path

spark = SparkSession.builder.appName('SparkByExamples.com').config('spark.driver.bindAddress','localhost').config("spark.ui.port","4050").getOrCreate()

merge_new_data(
    output_path=raw_data_flatten_path,
    historical_data_prefix=raw_data_dowload_path,
    new_data_prefix=raw_30_days_data_dowload_path,
    spark=spark)