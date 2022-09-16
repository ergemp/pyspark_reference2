from pyspark.sql import SparkSession

spark = SparkSession.builder() \
            .master("local[1]") \
            .appName("SparkByExamples.com") \
            .getOrCreate()
'''
master() – If you are running it on the cluster you need to use your master name as an argument to master()

Use local[x] when running in Standalone mode. x should be an integer value and should be greater than 0; 
this represents how many partitions it should create when using RDD, DataFrame, and Dataset. 
Ideally, x value should be the number of CPU cores you have.

appName() – Used to set your application name.

getOrCreate() – This returns a SparkSession object if already exists, and creates a new one if not exist.

Note: Creating SparkSession object, internally creates one SparkContext per JVM.
'''



