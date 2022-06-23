from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('create_rdd_from_text_file2').getOrCreate()
sc = spark.sparkContext

lines = sc.textFile('../data/category.csv')

# Load a text file and convert each line to a Row.
# lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.flatMap(lambda l: l.split(","))

parts.foreach(lambda e: print(e))