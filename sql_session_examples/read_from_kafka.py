from pyspark.sql import SparkSession

spark = SparkSession.builder.config('spark.jars', '../lib/spark-sql-kafka/spark-sql-kafka-0-10_2.12-3.2.1.jar,'
                                                  '../lib/spark-sql-kafka/kafka-clients-2.8.1.jar,'
                                                  '../lib/spark-sql-kafka/spark-token-provider-kafka-0-10_2.12-3.2.1.jar,'
                                                  '../lib/spark-sql-kafka/commons-pool2-2.8.1.jar')\
        .getOrCreate()

df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "10.211.55.9:9092,10.211.55.10:9092,10.211.55.11:9092") \
        .option("subscribe", "mytopic") \
        .load()

def func_call(df, batch_id):
    df.selectExpr("CAST(value AS STRING) as json")
    requests = df.rdd.map(lambda x: x.value).collect()

    # print (requests)

    for request in requests:
        # print(str(request) + "\n")
        print(request.decode("utf-8") + "")

# df.writeStream.outputMode('append').format('console').start()
# df.writeStream.outputMode('append').format('console').start().awaitTermination()
# df.writeStream.trigger(processingTime="10 second").outputMode('append').format('console').start().awaitTermination()
df.writeStream.trigger(processingTime="10 second").foreach(lambda event: print(event)).start().awaitTermination()
# df.writeStream.trigger(processingTime="10 second").foreachBatch(lambda batch,batchId: print(batch.count())).start().awaitTermination()
# df.writeStream.trigger(processingTime="10 seconds").foreachBatch(func_call).start().awaitTermination()






