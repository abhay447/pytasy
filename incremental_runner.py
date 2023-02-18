from etl.datawriter import merge_new_data
from path_manager import raw_data_dowload_path, raw_30_days_data_dowload_path, raw_data_flatten_path
from etl.spark.spark_session_helper import spark

merge_new_data(
    output_path=raw_data_flatten_path,
    historical_data_prefix=raw_data_dowload_path,
    new_data_prefix=raw_30_days_data_dowload_path,
    spark=spark)