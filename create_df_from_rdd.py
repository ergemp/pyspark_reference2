from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd = spark.sparkContext.parallelize(dataList)

dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()