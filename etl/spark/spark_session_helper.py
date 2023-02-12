from pyspark.sql import SparkSession


spark: SparkSession = SparkSession.builder.appName('SparkByExamples.com')\
    .config('spark.driver.bindAddress','localhost')\
    .config("spark.ui.port","4051")\
    .config("spark.driver.memory","8g")\
    .config("spark.sql.hive.filesourcePartitionFileCacheSize", "362144000000") \
    .getOrCreate()