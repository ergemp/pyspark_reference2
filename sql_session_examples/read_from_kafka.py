from pyspark.sql import SparkSession

spark = SparkSession.builder.config('spark.jars', '../lib/spark-sql-kafka/spark-sql-kafka-0-10_2.12-3.2.1.jar,'
                                                  '../lib/spark-sql-kafka/kafka-clients-2.8.1.jar,'
                                                  '../lib/spark-sql-kafka/spark-token-provider-kafka-0-10_2.12-3.2.1.jar,'
                                                  '../lib/spark-sql-kafka/commons-pool2-2.8.1.jar')\
        .getOrCreate()

df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "testtopic") \
        .load()

# df.writeStream.outputMode('append').format('console').start()

df.writeStream.outputMode('append').format('console').start().awaitTermination()