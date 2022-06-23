from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType


spark = SparkSession.builder.appName("create_df_from_csv_file").getOrCreate()

df = spark.read.option("delimiter", ",").option("inferSchema", True).option("header", True).csv("data/category.csv")

df.printSchema()

#

df = spark.read.csv("data/zipcodes.csv")
df.printSchema()

df = spark.read.format("csv").load("data/zipcodes.csv")
# or
# df = spark.read.format("org.apache.spark.sql.csv").load("data/zipcodes.csv")
df.printSchema()

df2 = spark.read.option("header", True).csv("data/zipcodes.csv")
df2.printSchema()

# reading multiple csv files
# df = spark.read.csv("path1,path2,path3")
# df = spark.read.csv("Folder path")

# options


df3 = spark.read.options(delimiter=',').csv("data/zipcodes.csv")
df4 = spark.read.options(inferSchema='True', delimiter=',').csv("data/zipcodes.csv")
# or

df4 = spark.read.option("inferSchema", True) \
                .option("delimiter", ",") \
                .csv("data/zipcodes.csv")

df5 = spark.read.options(header='True', inferSchema='True', delimiter=',') \
                .csv("data/zipcodes.csv")

# other options:
# quotes
# nullValues
# dateFormat

# reading csv files with a user specified custom schema

schema = StructType() \
    .add("RecordNumber", IntegerType(), True) \
    .add("Zipcode", IntegerType(), True) \
    .add("ZipCodeType", StringType(), True) \
    .add("City", StringType(), True) \
    .add("State", StringType(), True) \
    .add("LocationType", StringType(), True) \
    .add("Lat", DoubleType(), True) \
    .add("Long", DoubleType(), True) \
    .add("Xaxis", IntegerType(), True) \
    .add("Yaxis", DoubleType(), True) \
    .add("Zaxis", DoubleType(), True) \
    .add("WorldRegion", StringType(), True) \
    .add("Country", StringType(), True) \
    .add("LocationText", StringType(), True) \
    .add("Location", StringType(), True) \
    .add("Decommisioned", BooleanType(), True) \
    .add("TaxReturnsFiled", StringType(), True) \
    .add("EstimatedPopulation", IntegerType(), True) \
    .add("TotalWages", IntegerType(), True) \
    .add("Notes", StringType(), True)

df_with_schema = spark.read.format("csv") \
    .option("header", True) \
    .schema(schema) \
    .load("data/zipcodes.csv")

df_with_schema.printSchema()

#
# write as csv options
#

df_with_schema.write.option("header", True).csv("output/zipcodes.csv")
df_with_schema.write.options(header='True', delimiter=',').csv("output/zipcodes.csv")

# save modes
# overwrite
# append
# ignore: ignores write operation when the file already exists
# error: default option.


df2.write.mode('overwrite').csv("output/zipcodes.csv")
# or
df2.write.format("csv").mode('overwrite').save("output/zipcodes.csv")





