from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("reduce_example").getOrCreate()

rdd = spark.sparkContext.textFile("../data/clickStream.json")

# reduce() – Reduces the records to single, we can use this to count or sum.
print(rdd.reduce(lambda a, b: 1))

