from etl.datawriter import write_bulk_historical_data
from pyspark.sql import SparkSession

raw_data_prefix = '/home/abhay/work/dream11/downloads/raw_historical_data'
output_base_path = '/home/abhay/work/dream11/processed_output'

spark = SparkSession.builder.appName('SparkByExamples.com').config('spark.driver.bindAddress','localhost').config("spark.ui.port","4050").getOrCreate()
write_bulk_historical_data(raw_data_prefix,output_base_path,spark)