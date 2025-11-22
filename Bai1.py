from pyspark.sql import SparkSession

spark = SparkSession.builder \
	.appName("Test Spark") \
	.getOrCreate()

print("Spark version:", spark.version)
spark.stop()
