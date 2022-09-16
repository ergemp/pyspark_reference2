from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName('streaming_context_example3').config('spark.jars', '../lib/spark-sql-kafka/spark-sql-kafka-0-10_2.12-3.2.1.jar,'
                                                  '../lib/spark-sql-kafka/kafka-clients-2.8.1.jar,'
                                                  '../lib/spark-sql-kafka/spark-token-provider-kafka-0-10_2.12-3.2.1.jar,'
                                                  '../lib/spark-sql-kafka/commons-pool2-2.8.1.jar').getOrCreate()

# start streaming every one second
ssc = StreamingContext(spark.sparkContext, 1)

rddQueue = []
inputStream = ssc.queueStream(rddQueue)


