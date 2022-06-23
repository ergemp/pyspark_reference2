from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

spark = SparkSession.builder.appName('create_rdd_from_text_file2').getOrCreate()
sc = spark.sparkContext

lines = sc.textFile('data/category.csv')

# Load a text file and convert each line to a Row.
# lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))

# Each line is converted to a tuple.
categories = parts.map(lambda p: (p[0], p[1].strip()))

fields = [StructField("Category",StringType()) ,
          StructField("SubCategory",StringType()) ]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaCategories = spark.createDataFrame(categories, schema)

schemaCategories.printSchema()
schemaCategories.show()