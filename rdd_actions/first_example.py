from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("first_example").getOrCreate()

rdd = spark.sparkContext.textFile("../data/clickStream.json")

print(rdd.first())

