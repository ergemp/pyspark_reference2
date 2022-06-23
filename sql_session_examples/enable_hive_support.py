from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("enable_hive_support") \
    .master("local[2]") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport()

'''
do we really need that ?? 
yes, if you dont have a installed hive instance 
.config("spark.sql.warehouse.dir", "spark-warehouse") 
'''

df = spark.sql("select * from any_hive_table")