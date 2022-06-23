from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr


def upperCase(str):
    return str.upper()


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["Seqno","Name"]

data = [("1", "john jones"),
        ("2", "tracey smith"),
        ("3", "amy sanders")]

df = spark.createDataFrame(data=data, schema=columns)

df.show(truncate=False)

convertUDF = udf(lambda z: convertCase(z), StringType())

upperCaseUDF = udf(lambda z:upperCase(z), StringType())

''' 
StringType() is by default hence not required 
convertUDF = udf(lambda z: convertCase(z)) 
'''

df.select(col("Seqno"), \
    convertUDF(col("Name")).alias("Name") ) \
   .show(truncate=False)


df.withColumn("Cureated Name", upperCaseUDF(col("Name"))) \
  .show(truncate=False)

# using UDF in SQL

spark.udf.register("convertUDF", convertCase, StringType())
df.createOrReplaceTempView("NAME_TABLE")

spark.sql("select Seqno, convertUDF(Name) as Name from NAME_TABLE").show(truncate=False)





