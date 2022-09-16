from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('create_rdd_from_text_file2').getOrCreate()
sc = spark.sparkContext

lines = sc.textFile('../data/category.csv')

# Load a text file and convert each line to a Row.
# lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.flatMap(lambda l: l.split(","))

parts.foreach(lambda e: print(e))

'''
flatMap() transformation flattens the RDD after applying the function and returns a new RDD. 
On the below example, first, it splits each record by space in an RDD and finally flattens it. 
Resulting RDD consists of a single word on each record.
'''
