from typing import List
from pyspark.sql import SparkSession
import glob, os
from etl.datareader import read_match_file
from pytasy_cricket.flattened_delivery_record_schema import flattened_record_schema
from pyspark.sql import DataFrame
#Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()


def create_match_file_list(raw_data_prefix:str):
    os.chdir(raw_data_prefix)
    match_file_list: List[str] = []
    for file in glob.glob("*.json"):
        full_match_file = raw_data_prefix + "/" + file
        match_file_list.append(full_match_file)
    return match_file_list


def write_match_file(full_match_file: str, output_base_path: str) -> None:
    df: DataFrame = spark.createDataFrame(read_match_file(full_match_file), schema=flattened_record_schema)
    output_path:str  = output_base_path + "/delivery_parquet"
    df.write.format("parquet").partitionBy("year","month","dt").mode("append").save(output_path)