from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark = SparkSession \
    .builder \
    .appName("file_streaming_example") \
    .getOrCreate()

# Read all the csv files written atomically in a directory
userSchema = StructType().add("name", "string").add("age", "integer")
csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("/path/to/directory")  # Equivalent to format("csv").load("/path/to/directory")

