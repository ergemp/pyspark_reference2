from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

spark = SparkSession.builder.appName('map_example').getOrCreate()
sc = spark.sparkContext

lines = sc.textFile('../data/category.csv')

lines \
    .flatMap(lambda each: each.split(",")) \
    .map(lambda each: (each, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda x: (x[1], x[0])) \
    .repartition(1) \
    .sortByKey() \
    .foreach(lambda each: print(each))


'''
reduceByKey() merges the values for each key with the function specified. 
In our example, it reduces the word string by applying the sum function on value. 
The result of our RDD contains unique words and their count. 
'''
