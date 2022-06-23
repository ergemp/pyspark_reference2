from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .config("spark.sql.warehouse.dir", "file:///tmp/spark_warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.option("header", True).csv("../data/category.csv")
df.createOrReplaceTempView("category")

# Create Hive table & query it.
spark.table("category").write.saveAsTable("hiveCategory")

df3 = spark.sql("SELECT Category, SubCategory FROM hiveCategory")

df3.show()
