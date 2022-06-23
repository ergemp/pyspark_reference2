from pyspark.sql.types import StringType, MapType

mapCol = MapType(StringType(), StringType(), False)

'''
The First param keyType is used to specify the type of the key in the map.
The Second param valueType is used to specify the type of the value in the map.
Third parm valueContainsNull is an optional boolean type that is used to specify if the value of the second param can accept Null/None values.
The key of the map won’t accept None/Null values.
PySpark provides several SQL functions to work with MapType.
'''

# create map type from struct type
from pyspark.sql.types import StructField, StructType, StringType, MapType

schema = StructType([
            StructField('name', StringType(), True),
            StructField('properties', MapType(StringType(), StringType()), True)
        ])

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dataDictionary = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]

df = spark.createDataFrame(data=dataDictionary, schema=schema)
df.printSchema()
df.show(truncate=False)

# Access PySpark MapType Elements

df3=df.rdd.map(lambda x: \
    (x.name, x.properties["hair"], x.properties["eye"])) \
    .toDF(["name", "hair", "eye"])

df3.printSchema()
df3.show()

# another way to get the value of a key from Map using getItem() of Column type,
# this method takes a key as an argument and returns a value.

df.withColumn("hair", df.properties.getItem("hair")) \
  .withColumn("eye", df.properties.getItem("eye")) \
  .drop("properties") \
  .show()

df.withColumn("hair", df.properties["hair"]) \
  .withColumn("eye", df.properties["eye"]) \
  .drop("properties") \
  .show()

# functions on mapType column

# explode

from pyspark.sql.functions import explode

df.select(df.name, explode(df.properties)).show()

# map_keys

from pyspark.sql.functions import map_keys

df.select(df.name, map_keys(df.properties)).show()

keysDF = df.select(explode(map_keys(df.properties))).distinct()
keysList = keysDF.rdd.map(lambda x: x[0]).collect()
print(keysList)

# map_values

from pyspark.sql.functions import map_values

df.select(df.name, map_values(df.properties)).show()

