from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName('streaming_context_example2').getOrCreate()

# start streaming every one second
ssc = StreamingContext(spark.sparkContext, 1)
