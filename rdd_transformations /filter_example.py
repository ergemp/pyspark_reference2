from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

spark = SparkSession.builder.appName('create_rdd_from_text_file2').getOrCreate()
sc = spark.sparkContext

lines = sc.textFile('../data/category.csv')

print(f'count without filter : {lines.count()}')
print(f'count with filter : {lines.filter(lambda each: "Electronics" not in each).count()}')