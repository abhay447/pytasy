from etl.datawriter import merge_new_data
from pyspark.sql import SparkSession

raw_data_prefix = '/home/abhay/work/dream11/downloads/raw_historical_data'
new_data_prefix = '/home/abhay/work/dream11/downloads/last_30_days_data'
output_base_path = '/home/abhay/work/dream11/processed_output'


spark = SparkSession.builder.appName('SparkByExamples.com').config('spark.driver.bindAddress','localhost').config("spark.ui.port","4050").getOrCreate()

merge_new_data(raw_data_prefix=raw_data_prefix,output_base_path=output_base_path,new_data_prefix=new_data_prefix,spark=spark)