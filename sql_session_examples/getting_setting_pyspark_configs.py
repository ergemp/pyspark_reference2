
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Set Config
spark.conf.set("spark.executor.memory", "5g")

# Get a Spark Config
partions = spark.conf.get("spark.sql.shuffle.partitions")

print(partions)