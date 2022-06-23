from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructType, StructField


spark = SparkSession.builder.getOrCreate()
rdd = spark.read.text("data/category.csv").rdd

#rdd.foreach(lambda f: print(f))

rdd2 = rdd.flatMap(lambda f : f.value.split(","))

rdd2.foreach(lambda f: print(f))
rdd2 = rdd2.map(lambda f: Row(f))

fields = []

fields.append(StructField("allcategories", StringType()))

schema = StructType(fields)
df = spark.createDataFrame(rdd2, schema)

df.printSchema()
df.show()




