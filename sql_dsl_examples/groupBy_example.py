from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import countDistinct, avg, sum, mean, min, max, count, stddev, aggregate

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

spark = SparkSession.builder.getOrCreate()

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)

df.printSchema()

df.show(truncate=False)

# groupBy sum example

df.groupBy("department").sum("salary").show(truncate=False)

# group by count example

df.groupBy("department").count().show()

#  group by min example

df.groupBy("department").min("salary").show()

# group by max example

df.groupBy("department").max("salary").show()

# group by avg example

df.groupBy("department").avg("salary").show()

# group by mean example

df.groupBy("department").mean("salary").show()

# groupBy and aggregate on multiple columns

df.groupBy("department","state") \
    .sum("salary","bonus") \
    .show(truncate = False)

# Running more aggregates at a time

df.groupBy("department") \
    .agg(sum("salary").alias("sumSalary"), \
         avg("salary"),
         sum("bonus"),
         max("bonus")
     ) \
    .show(truncate=False)

# Using filter on aggregate data

df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
      avg("salary").alias("avg_salary"), \
      sum("bonus").alias("sum_bonus"), \
      max("bonus").alias("max_bonus")) \
    .where(col("sum_bonus") >= 50000) \
    .show(truncate=False)






