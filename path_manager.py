import os

base_path = os.path.dirname(os.path.realpath(__file__))

print(base_path)

raw_data_dowload_path = base_path + '/downloads/raw_historical_data'
raw_data_flatten_path = base_path + '/processed_output/delivery_parquet'