from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.text('hdfs://localhost:8020/user/hive/warehouse/tt')

df.show()








