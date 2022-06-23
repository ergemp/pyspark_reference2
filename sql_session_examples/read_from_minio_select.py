from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
import boto3


spark = SparkSession.builder.config("spark.jars", "../lib/minio-select/spark-select_2.11-2.1.jar").getOrCreate()

s3 = boto3.client('s3', endpoint_url = 'http://localhost:9000', aws_access_key_id = 'minioadmin', aws_secret_access_key='minioadmin' )

ss = StructType([ \
    StructField("sid", StringType(), True), \
    StructField("pid", StringType(), True), \
    StructField("userId", StringType(), True), \
    StructField("ts", LongType(), True)
    ])

spark \
  .read \
  .format("minioSelectJSON") \
  .schema(ss) \
  .load("s3a://mockdata/clickStream.json")

# df = spark.read.load("s3a://mockdata/clickStream.json")

''' works only with scala version 2.11 '''