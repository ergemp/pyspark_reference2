from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.jars", "../lib/minio/aws-java-sdk-1.11.534.jar,"
                         "../lib/minio/aws-java-sdk-core-1.11.534.jar,"
                         "../lib/minio/aws-java-sdk-dynamodb-1.11.534.jar,"
                         "../lib/minio/aws-java-sdk-kms-1.11.534.jar,"
                         "../lib/minio/aws-java-sdk-s3-1.11.534.jar,"
                         "../lib/minio/hadoop-aws-3.1.2.jar,"
                         "../lib/minio/httpclient-4.5.3.jar,"
                         "../lib/minio/joda-time-2.9.9.jar").getOrCreate()

sc = spark.sparkContext

sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", 'true')
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", 'false')
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

lines = sc.textFile('s3a://mockdata/clickStream.json')

lines.foreach(lambda e: print(e))
print("total line count: " + str(lines.count()))

lines.repartition(1).saveAsTextFile("s3a://mockdata/clickStream2.json")







