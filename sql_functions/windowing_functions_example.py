# PySpark Window functions are used to calculate results such as the rank, row number e.t.c over a range of input rows.
# PySpark Window functions operate on a group of rows (like frame, partition)
# and return a single value for every input row.
#
# PySpark SQL supports three kinds of window functions:
#
# ranking functions
# analytic functions
# aggregate functions

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('windowing_functions_example').getOrCreate()

simpleData = (("James", "Sales", 3000),
              ("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("Maria", "Finance", 3000),
              ("James", "Sales", 3000),
              ("Scott", "Finance", 3300),
              ("Jen", "Finance", 3900),
              ("Jeff", "Marketing", 3000),
              ("Kumar", "Marketing", 2000),
              ("Saif", "Sales", 4100)
              )

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
df.show(truncate=False)


# we can use any existing aggregate functions as a window function.

# To perform an operation on a group first, we need to partition the data using Window.partitionBy(),
# and for row number and rank function we need to additionally order by on partition data using orderBy clause.

# row_number() window function is used to give the sequential row number
# starting from 1 to the result of each window partition.

windowSpec = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number", row_number().over(windowSpec)).show(truncate=False)

# rank() window function is used to provide a rank to the result within a window partition.
# This function leaves gaps in rank when there are ties.

df.withColumn("rank", rank().over(windowSpec)).show()

# dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps.
# This is similar to rank() function difference being rank function leaves gaps in rank when there are ties.

df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

# percent_rank() function

df.withColumn("percent_rank", percent_rank().over(windowSpec)).show()

# ntile() window function returns the relative rank of result rows within a window partition.
# In below example we have used 2 as an argument to ntile hence it returns ranking between 2 values (1 and 2)

df.withColumn("ntile", ntile(2).over(windowSpec)).show()

#
# window analytic functions
#

# cume_dist() window function is used to get the cumulative distribution of values within a window partition.
# This is the same as the DENSE_RANK function in SQL.

df.withColumn("cume_dist", cume_dist().over(windowSpec)).show()

# lag() functions
# This is the same as the LAG function in SQL.

df.withColumn("lag", lag("salary", 2).over(windowSpec)).show()

# lead() function
# This is the same as the LEAD function in SQL.

df.withColumn("lead", lead("salary", 2).over(windowSpec)).show()

#
# window aggregate functions
#

windowSpecAgg  = Window.partitionBy("department")

df.withColumn("row", row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row") == 1).select("department", "avg", "sum", "min", "max") \
  .show()


