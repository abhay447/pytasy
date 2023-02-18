from etl.spark.spark_session_helper import spark

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor ,GBTRegressionModel
from path_manager import model_train_input_path, model_test_input_path, model_train_predictions_path, model_test_predictions_path, model_save_artifact_path

features = [
     'is_male',
     'batter_runs_30D',
     'batter_runs_90D',
     'batter_runs_300D',
     'batter_runs_1000D',
     'batter_runs_1000D_venue',
     'balls_faced_30D',
     'balls_faced_90D',
     'balls_faced_300D',
     'balls_faced_1000D',
     'balls_faced_1000D_venue',
     'dismissals_30D',
     'dismissals_90D',
     'dismissals_300D',
     'dismissals_1000D',
     'dismissals_1000D_venue',
     'boundary_count_30D',
     'boundary_count_90D',
     'boundary_count_300D',
     'boundary_count_1000D',
     'boundary_count_1000D_venue',
     'six_count_30D',
     'six_count_90D',
     'six_count_300D',
     'six_count_1000D',
     'six_count_1000D_venue',
     'batting_avg_30D',
     'batting_avg_90D',
     'batting_avg_300D',
     'batting_avg_1000D',
     'batting_avg_1000D_venue',
     'batting_sr_30D',
     'batting_sr_90D',
     'batting_sr_300D',
     'batting_sr_1000D',
     'batting_sr_1000D_venue',
     'total_runs_30D',
     'total_runs_90D',
     'total_runs_300D',
     'total_runs_1000D',
     'total_runs_1000D_venue',
     'deliveries_30D',
     'deliveries_90D',
     'deliveries_300D',
     'deliveries_1000D',
     'deliveries_1000D_venue',
     'wicket_sum_30D',
     'wicket_sum_90D',
     'wicket_sum_300D',
     'wicket_sum_1000D',
     'wicket_sum_1000D_venue',
     'maiden_count_30D',
     'maiden_count_90D',
     'maiden_count_300D',
     'maiden_count_1000D',
     'maiden_count_1000D_venue',
     'bowling_avg_30D',
     'bowling_avg_90D',
     'bowling_avg_300D',
     'bowling_avg_1000D',
     'bowling_avg_1000D_venue',
     'bowling_sr_30D',
     'bowling_sr_90D',
     'bowling_sr_300D',
     'bowling_sr_1000D',
     'bowling_sr_1000D_venue',
     'bowling_eco_30D',
     'bowling_eco_90D',
     'bowling_eco_300D',
     'bowling_eco_1000D',
     'bowling_eco_1000D_venue'
]

def start_training():
     train_df = spark.read.parquet(model_train_input_path)
     test_df = spark.read.parquet(model_test_input_path)
     print("Starting Model Training Flow")
     # assemble features
     va = VectorAssembler(inputCols = features, outputCol='features')
     va_train_df = va.transform(train_df)

     print("Assembled train features")

     # train model 
     gbtr = GBTRegressor(featuresCol='features', labelCol='fantasy_points', maxIter=10)
     gbtr = gbtr.fit(va_train_df)

     print("model trained")

     # save model
     gbtr.write().overwrite().save(model_save_artifact_path)

     print("model saved")

     # load saved model
     loaded_gbtr = GBTRegressionModel.load(model_save_artifact_path)

     print("loaded saved model")

     # assemble test features and predict on test data
     va_test_df = va.transform(test_df)
     test_predictions = loaded_gbtr.transform(va_test_df)
     test_predictions.show(3)
     train_predictions = loaded_gbtr.transform(va_train_df)

     # write predictions to disk
     print("writing predictions over test input data to disk")
     test_predictions.write.format("parquet").partitionBy(["dt"]).mode("overwrite").save(model_test_predictions_path)

     print("writing predictions over train input data to disk")
     train_predictions.write.format("parquet").partitionBy(["dt"]).mode("overwrite").save(model_train_predictions_path)