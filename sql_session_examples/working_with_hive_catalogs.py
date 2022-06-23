from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .config("spark.sql.warehouse.dir", "file:///tmp/spark_warehouse") \
    .enableHiveSupport() \
    .getOrCreate()


dbs = spark.catalog.listDatabases()
tbls = spark.catalog.listTables()
tbls = spark.catalog.listTables("default")

print(dbs)
print(tbls)
