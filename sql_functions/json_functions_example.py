from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import MapType, StringType

spark = SparkSession.builder.appName('json_functions_example').getOrCreate()

jsonString = """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
df = spark.createDataFrame([(1, jsonString)], ["id", "value"])

df.show(truncate=False)

# from_json() example
# convert JSON string into Struct type or Map type.
# below example converts JSON string to Map key-value pair

df2 = df.withColumn("value", from_json(df.value, MapType(StringType(), StringType())))
df2.printSchema()
df2.show(truncate=False)

# to_json() example
# convert DataFrame columns MapType or Struct type to JSON string

df2.withColumn("value", to_json(col("value"))).show(truncate=False)

# json_tuple() example
# query or extract the elements from JSON column and create the result as a new columns.

df.select(col("id"), json_tuple(col("value"), "Zipcode", "ZipCodeType", "City")) \
    .toDF("id", "Zipcode", "ZipCodeType", "City") \
    .show(truncate=False)

# get_json_object() example
# extract the JSON string based on path from the JSON column.

df.select(col("id"), get_json_object(col("value"), "$.ZipCodeType").alias("ZipCodeType")) \
    .show(truncate=False)

# schema_of_json() example
# create schema string from JSON string column.

schemaStr = spark.range(1) \
    .select(schema_of_json(lit("""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""))) \
    .collect()[0][0]
print(schemaStr)

