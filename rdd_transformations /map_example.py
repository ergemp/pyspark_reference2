from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

spark = SparkSession.builder.appName('map_example').getOrCreate()
sc = spark.sparkContext

lines = sc.textFile('../data/category.csv')

lines\
    .map(lambda each: " tt " + each)\
    .foreach(lambda each: print(each))
