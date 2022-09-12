from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd = spark.sparkContext.parallelize(dataList)

columns = ["language","users_count"]
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()

dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
dfFromRDD2.printSchema()
