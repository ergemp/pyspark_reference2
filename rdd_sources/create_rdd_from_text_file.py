from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('create_rdd_from_text_file').getOrCreate()
sc = spark.sparkContext

lines = sc.textFile('data/category.csv')
parts = lines.map(lambda e: e.split(","))
parts.foreach(lambda e: print (e))
categories = parts.map(lambda p: Row(category=p[0], subcategory=p[1]))

categoriesSchema = spark.createDataFrame(categories)

categoriesSchema.printSchema()
print(categoriesSchema.count())




