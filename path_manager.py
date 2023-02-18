import os

base_path = os.path.dirname(os.path.realpath(__file__))

print(base_path)

raw_data_dowload_path = base_path + '/downloads/raw_historical_data'
raw_30_days_data_dowload_path = base_path + '/downloads/last_30_days_data'
raw_data_flatten_path = base_path + '/processed_output/delivery_parquet'

intermediate_data_all_train_rows_path = base_path + '/processed_output/training_rows'
intermediate_data_t20_bowler_match_path = base_path + '/processed_output/t20_bowler_match_stats'
intermediate_data_t20_batter_match_path = base_path + '/processed_output/t20_batter_match_stats'
intermediate_data_t20_fielder_match_path = base_path + '/processed_output/t20_fielder_match_stats'

model_train_input_path = base_path + '/model_data/model_inputs/train'
model_test_input_path = base_path + '/model_data/model_inputs/test'

model_save_artifact_path =  base_path + '/model_data/models/GbtRegression/v1'

model_train_predictions_path = base_path + '/model_data/model_oututs/train'
model_test_predictions_path = base_path + '/model_data/model_oututs/test'