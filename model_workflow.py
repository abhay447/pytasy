from dataprep import prepare_data
from model_trainer import start_training
from model_ranking_evaluator import evaluate_model_ranking

DATA_DOWNLOAD_MODE = "FULL" # FULL or INCREMENTAL
# when INCREMENTAL is chosen then it is assumed that historical data already exists and last 30 days data is fetched and merged to that


print("Start data prep")
prepare_data(DATA_DOWNLOAD_MODE)

print("Start training")
start_training()

print("Evaluate Ranking Models")
evaluate_model_ranking()