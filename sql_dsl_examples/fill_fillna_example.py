from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

filePath = "../data/small_zipcodes.csv"

df = spark.read.options(header='true', inferSchema='true').csv(filePath)

df.printSchema()
df.show(truncate=False)

# Replace 0 for null for all integer columns
df.na.fill(value=0).show()

# Replace 0 for null on only population column
df.na.fill(value=0, subset=["population"]).show()

'''
Above both statements yields the same output, 
since we have just an integer column population with null values 
Note that it replaces only Integer columns since our value is 0.
'''

df.na.fill("").show(truncate=False)

df.na.fill("unknown", ["city"]) \
    .na.fill("", ["type"]).show()

df.na.fill({"city": "unknown", "type": ""}).show()


