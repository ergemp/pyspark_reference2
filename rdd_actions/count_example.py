from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("count_example").getOrCreate()

rdd = spark.sparkContext.textFile("../data/clickStream.json")

print(rdd.count())
