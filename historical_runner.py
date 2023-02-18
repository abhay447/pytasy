from etl.datawriter import write_bulk_historical_data
from pyspark.sql import SparkSession
from path_manager import raw_data_dowload_path, raw_data_flatten_path

spark = SparkSession.builder.appName('SparkByExamples.com').config('spark.driver.bindAddress','localhost').config("spark.ui.port","4050").getOrCreate()
write_bulk_historical_data(raw_data_dowload_path,raw_data_flatten_path,spark)