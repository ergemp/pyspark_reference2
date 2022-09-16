from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

spark = SparkSession.builder.appName('map_example').getOrCreate()
sc = spark.sparkContext

lines = sc.textFile('../data/category.csv')

lines\
    .map(lambda each: " tt " + each)\
    .foreach(lambda each: print(each))

'''
map – map() transformation is used the apply any complex operations like adding a column, updating a column e.t.c, 
the output of map transformations would always have the same number of records as input.
'''