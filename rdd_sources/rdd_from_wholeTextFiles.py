from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

spark = SparkSession.builder.appName('create_rdd_from_wholeTextFiles').getOrCreate()
sc = spark.sparkContext

'''
wholeTextFiles() function returns a PairRDD with the key being the file path and value being file content.
'''
lines = sc.wholeTextFiles('../data/category.csv')

