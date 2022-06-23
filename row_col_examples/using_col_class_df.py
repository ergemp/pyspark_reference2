from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Row
import sys

spark = SparkSession.builder.getOrCreate()

data=[Row(fname = "James", sname="Bond", id="100", gender=None),
      Row(fname="Ann", sname="Varsa", id="200", gender='F'),
      Row(fname="Tom Cruise",sname="XXX", id="400", gender=''),
      Row(fname="Tom Brand",sname=None,id="400",gender='M')]

columns=["fname","lname","id","gender"]

df=spark.createDataFrame(data)
df.printSchema()

'''
root
 |-- fname: string (nullable = true)
 |-- sname: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
'''

# Using DataFrame object (df)
df.select(df.gender).show()
df.select(df["gender"]).show()

# Using SQL col() function
df.select(col("gender")).show()

# Accessing column name with dot (with backticks)
df.select(col("`fname`")).show()


'''
'''


data=[Row(name="James",prop=Row(hair="black",eye="blue")),
      Row(name="Ann",prop=Row(hair="grey",eye="black"))]

df=spark.createDataFrame(data)
df.printSchema()
#root
# |-- name: string (nullable = true)
# |-- prop: struct (nullable = true)
# |    |-- hair: string (nullable = true)
# |    |-- eye: string (nullable = true)

#Access struct column
df.select(df.prop.hair).show()
df.select(df["prop.hair"]).show()
df.select(col("prop.hair")).show()

#Access all columns from struct
df.select(col("prop.*")).show()

