from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext

lines = sc.textFile('hdfs://localhost:8020/user/hive/warehouse/tt')

lines.foreach(lambda e: print(e))
print("total line count: " + str(lines.count()))








