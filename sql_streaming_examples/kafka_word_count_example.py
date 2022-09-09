from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, StringType, TimestampType

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
        .load() \
        .selectExpr("CAST(value AS STRING)", "cast(timestamp as timestamp)")

'''
@udf(returnType=ArrayType(StringType()))
def my_split2(x):
    return x.split(' ')
'''

# explode turns each item in an array into a separate row
# df.value.split(' ')  # the warning occurred
'''
words = df \
    .select(explode(split(df.value, ' ')).alias('word'), 'timestamp') \
    .groupby(window(df.timestamp, "10 seconds"), 'word') \
    .count() \
    .writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode('complete') \
    .format('console') \
    .foreach(lambda each: print(each)) \
    .start() \
    .awaitTermination()
'''

words = df \
    .select(explode(split(df.value, ' ')).alias('word'), 'timestamp') \
    .groupby(window(df.timestamp, "10 seconds"), 'word') \
    .count() \
    .orderBy('count', ascending=False) \
    .writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode('complete') \
    .option("truncate", False) \
    .format('console') \
    .start() \
    .awaitTermination()

# wordCounts = words.groupby('word').count().orderBy('count',ascending=False)
'''
wordCounts.writeStream \
    .trigger(processingTime="10 seconds") \
    .outputMode('complete') \
    .format('console') \
    .start() \
    .awaitTermination()
'''

# Generate running word count
# wordCounts = words.groupBy('word').count()
