from pyspark.sql import SparkSession

spark = SparkSession.builder.config('spark.jars', '../lib/spark-sql-kafka/spark-sql-kafka-0-10_2.12-3.2.1.jar,'
                                                  '../lib/spark-sql-kafka/kafka-clients-2.8.1.jar,'
                                                  '../lib/spark-sql-kafka/spark-token-provider-kafka-0-10_2.12-3.2.1.jar,'
                                                  '../lib/spark-sql-kafka/commons-pool2-2.8.1.jar') \
                            .config('spark.sql.debug.maxToStringFields', '100') \
                            .appName("test") \
                            .getOrCreate()

# to set the maximum number of messages per partition per batch.
# .config('spark.streaming.kafka.maxRatePerPartition','10')

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("kafka.security.protocol", "SSL") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "testtopic") \
    .option("includeHeaders", "true") \
    .option("startingOffsets", "latest") \
    .option("spark.streaming.kafka.maxRatePerPartition", "50") \
    .load()

# .option("subscribePattern", "testtopic*") \
# .option("startingOffsets", "earliest") \

# df.writeStream.outputMode('append').format('console').start()
# kafka_df.writeStream.outputMode('append').format('console').start().awaitTermination()

#
#
#

import logging


def func_call(df, batch_id):
    df.selectExpr("CAST(value AS STRING) as json")
    requests = df.rdd.map(lambda x: x.value).collect()
    logging.info(requests)


query = kafka_df.writeStream \
            .foreachBatch(func_call) \
            .option("checkpointLocation", "file://F:/tmp/kafka/checkpoint") \
            .trigger(processingTime="5 minutes") \
            .start().awaitTermination()

# kafka producer with pyspark
'''
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
    .option("kafka.security.protocol", "SSL") \
    .option("failOnDataLoss", "false") \
    .option("topic", "topic2") \
    .save()
'''

