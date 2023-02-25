from pyspark.sql import SparkSession


spark: SparkSession = SparkSession.builder.appName('SparkByExamples.com')\
    .master("local[*]")\
    .config("spark.ui.port","4051")\
    .config("spark.driver.memory","8g")\
    .config("spark.sql.hive.filesourcePartitionFileCacheSize", "362144000000") \
    .config('spark.jars.packages', 'org.jpmml:pmml-sparkml:2.1.0') \
    .getOrCreate()