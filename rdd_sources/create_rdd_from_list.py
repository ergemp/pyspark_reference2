from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Create RDD from parallelize
dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd = sc.parallelize(dataList)

rdd.foreach(lambda each: print(each))

'''
parallelize() is a function in SparkContext and is used to create an RDD from a list collection.
'''
rdd2 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

rddCollect = rdd2.collect()
print("Number of Partitions: " + str(rdd.getNumPartitions()))
print("Action: First element: " + str(rdd2.first()))
print(rddCollect)

#
# creating an empty rdd
#

emptyRDD = sc.emptyRDD()
emptyRDD2 = rdd = sc.parallelize([])

print("is Empty RDD : " + str(emptyRDD2.isEmpty()))


