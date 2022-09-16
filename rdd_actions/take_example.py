from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("take_example").getOrCreate()

rdd = spark.sparkContext.textFile("../data/clickStream.json")

'''
take() – Returns the record specified as an argument.
'''

print(len(rdd.take(100)))
