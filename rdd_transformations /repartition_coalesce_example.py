from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('repartition_coalesce_example').getOrCreate()

'''
repartition() is used to increase or decrease the RDD/DataFrame partitions 
whereas the PySpark coalesce() is used to only decrease the number of partitions in an efficient way.

In RDD, you can create parallelism at the time of the creation of an RDD 
using parallelize(), textFile() and wholeTextFiles().
'''


rdd = spark.sparkContext.parallelize((0, 20))
print("From local[5] " + str(rdd.getNumPartitions()))

rdd1 = spark.sparkContext.parallelize((0, 25), 6)
print("parallelize : " + str(rdd1.getNumPartitions()))

rddFromFile = spark.sparkContext.textFile("../data/clickStream.json", 10)
print("TextFile : " + str(rddFromFile.getNumPartitions()))

# rdd1.saveAsTextFile("../output/partition")

'''
repartition() method is used to increase or decrease the partitions. 
The below example decreases the partitions from 10 to 4 by moving data from all partitions.
'''

rdd2 = rdd1.repartition(4)
print("Repartition size : " + str(rdd2.getNumPartitions()))
# rdd2.saveAsTextFile("../output/re-partition")

'''
coalesce() is used only to reduce the number of partitions. 
This is an optimized or improved version of repartition() where 
the movement of the data across the partitions is lower using coalesce.
'''

rdd3 = rdd1.coalesce(4)
print("Repartition size : " + str(rdd3.getNumPartitions()))
# rdd3.saveAsTextFile("../output/coalesce")


