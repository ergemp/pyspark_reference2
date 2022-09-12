from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


df = spark.read.json('data/clickStream.json')

df.createOrReplaceTempView("clickStream")

df.printSchema()

df.groupBy("event").count().show()
spark.sql("select count(*) as count, event from clickStream group by event").show()

#

df = spark.read.json("data/zipcodes.json")
df.printSchema()
df.show()

# multi line json example
# multiline_df = spark.read.option("multiline", "true").json("resources/multiline-zipcode.json")
# multiline_df.show()

# read multiple files example
# df2 = spark.read.json(['data/zipcode1.json', 'data/zipcode2.json'])
# df3 = spark.read.json("data/*.json")

# read with custom schema

from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType, BooleanType

schema = StructType([
      StructField("RecordNumber",IntegerType(),True),
      StructField("Zipcode",IntegerType(),True),
      StructField("ZipCodeType",StringType(),True),
      StructField("City",StringType(),True),
      StructField("State",StringType(),True),
      StructField("LocationType",StringType(),True),
      StructField("Lat",DoubleType(),True),
      StructField("Long",DoubleType(),True),
      StructField("Xaxis",IntegerType(),True),
      StructField("Yaxis",DoubleType(),True),
      StructField("Zaxis",DoubleType(),True),
      StructField("WorldRegion",StringType(),True),
      StructField("Country",StringType(),True),
      StructField("LocationText",StringType(),True),
      StructField("Location",StringType(),True),
      StructField("Decommisioned",BooleanType(),True),
      StructField("TaxReturnsFiled",StringType(),True),
      StructField("EstimatedPopulation",IntegerType(),True),
      StructField("TotalWages",IntegerType(),True),
      StructField("Notes",StringType(),True)
  ])

df_with_schema = spark.read.schema(schema).json("data/zipcodes.json")
df_with_schema.printSchema()
df_with_schema.show()

# read with pyspark SQL

spark.sql("CREATE OR REPLACE TEMPORARY VIEW zipcode USING json OPTIONS (path 'data/zipcodes.json')")
spark.sql("select * from zipcode").show()

# options:
# nullValues
# dateFormat

#
# writing to json file examples
#

df_with_schema.write.mode('Overwrite').json("output/zipcodes.json")




