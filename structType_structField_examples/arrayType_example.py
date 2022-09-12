from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType

spark = SparkSession.builder.master("local[1]") \
    .appName('arrayType_example') \
    .getOrCreate()

# defining an array column

arrayCol = ArrayType(StringType(), False)
'''
This takes arguments valueType and one optional argument valueContainsNull to specify if a value can accept null, 
by default it takes True. valueType should be a PySpark type that extends DataType class
'''

arrayStructureSchema = StructType([
    StructField('name', StructType([
       StructField('firstname', StringType(), True),
       StructField('middlename', StringType(), True),
       StructField('lastname', StringType(), True)
       ])),
    StructField('hobbies', ArrayType(StringType()), True),
    StructField('properties', MapType(StringType(),StringType()), True)
    ])

print(arrayStructureSchema.json())

#
#
#

data = [("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
        ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
        ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")]

schema = StructType([
    StructField("name",StringType(),True),
    StructField("languagesAtSchool",ArrayType(StringType()),True),
    StructField("languagesAtWork",ArrayType(StringType()),True),
    StructField("currentState", StringType(), True),
    StructField("previousState", StringType(), True)
  ])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show()

# https://sparkbyexamples.com/spark/spark-sql-array-functions/

# explode example

from pyspark.sql.functions import explode

df.select(df.name, explode(df.languagesAtSchool)).show()

# split example

from pyspark.sql.functions import split

df.select(split(df.name, ",").alias("nameAsArray")).show()

# array example

from pyspark.sql.functions import array

df.select(df.name, array(df.currentState, df.previousState).alias("States")).show()

# array contains example

from pyspark.sql.functions import array_contains

df.select(df.name, array_contains(df.languagesAtSchool, "Java").alias("array_contains")).show()

