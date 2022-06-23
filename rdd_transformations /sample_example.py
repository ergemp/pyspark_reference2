from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

rdd = spark.sparkContext.range(0, 100)

print(rdd.sample(False, 0.1, 0).collect())

print(rdd.sample(True, 0.3, 123).collect())

# takeSample example


print(rdd.takeSample(False,10,0))

print(rdd.takeSample(True,30,123))