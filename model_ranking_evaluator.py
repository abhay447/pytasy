from etl.spark.spark_session_helper import spark

output_test_predictions_path = "/home/abhay/work/dream11/model_data/model_oututs/test"

# read test predictions
test_df = spark.read.parquet(output_test_predictions_path)
test_df.registerTempTable("test_preds")

print("Calculating player ranking metrics")
print("******************************************************************************************************")

# calculate ranks based on fantasy scores
rank_comparison_df = spark.sql("""
with actual_ranks as (
    select player_id, match_id, 
        fantasy_points, rank() over(partition by match_id order by fantasy_points desc) as actual_rank
    from test_preds
)
, predicted_ranks as  (
    select player_id, match_id, 
         prediction, rank() over(partition by match_id order by prediction desc) as predicted_rank
    from test_preds
)
select actual_ranks.player_id, actual_ranks.match_id, actual_ranks.fantasy_points, actual_ranks.actual_rank, 
    predicted_ranks.prediction, predicted_ranks.predicted_rank
from actual_ranks as actual_ranks 
inner join predicted_ranks as predicted_ranks
where actual_ranks.player_id = predicted_ranks.player_id 
and actual_ranks.match_id = predicted_ranks.match_id 
""")
rank_comparison_df.registerTempTable("compared_ranks")

sample_ranks_df = spark.sql("""select * from compared_ranks order by match_id, player_id, actual_rank""").limit(100).toPandas() 
print(sample_ranks_df.to_markdown())
# type: ignoreprint("Sample Ranks")

print("******************************************************************************************************")

# compare probability of model giving correct top 11 players
probability_of_predicting_top_11_players = spark.sql("""
    select
        (select count(*) from compared_ranks where actual_rank <12 and predicted_rank < 12)/(select count(*) from compared_ranks where actual_rank <12) as probability_of_predicting_top_11_players
""").toPandas()
print(probability_of_predicting_top_11_players.to_markdown())

print("******************************************************************************************************")

# compare probability of model giving correct top 2 players, helpful in choosing captain and vice captain
probability_of_predicting_top_2_players = spark.sql("""
    select
        (select count(*) from compared_ranks where actual_rank <3 and predicted_rank < 3)/(select count(*) from compared_ranks where actual_rank <3) as probability_of_predicting_top_2_players
""").toPandas()
print(probability_of_predicting_top_2_players.to_markdown())

print("******************************************************************************************************")

# NDCG

ndcg = spark.sql("""
with gain_matrix as (
    select
        match_id, player_id, 
        fantasy_points/log2(actual_rank + 1) as ideal_discounted_gain, 
        prediction/log2(predicted_rank + 1) as discounted_gain from compared_ranks
)
select sum(discounted_gain)/sum(ideal_discounted_gain) as ndcg from gain_matrix
""").toPandas()
print(ndcg.to_markdown())

print("******************************************************************************************************")