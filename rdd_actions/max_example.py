from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("max_example").getOrCreate()

rdd = spark.sparkContext.textFile("../data/clickStream.json")

print(rdd.max())



