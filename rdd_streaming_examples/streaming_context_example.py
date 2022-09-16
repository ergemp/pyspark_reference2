from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "streaming_context_example")

# start streaming every one second
ssc = StreamingContext(sc, 1)



