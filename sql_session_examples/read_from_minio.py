from pyspark.sql import SparkSession

spark = SparkSession.builder.\
    config("spark.jars", "../lib/minio/aws-java-sdk-1.11.534.jar,"
                         "../lib/minio/aws-java-sdk-core-1.11.534.jar,"
                         "../lib/minio/aws-java-sdk-dynamodb-1.11.534.jar,"
                         "../lib/minio/aws-java-sdk-kms-1.11.534.jar,"
                         "../lib/minio/aws-java-sdk-s3-1.11.534.jar,"
                         "../lib/minio/hadoop-aws-3.1.2.jar,"
                         "../lib/minio/httpclient-4.5.3.jar,"
                         "../lib/minio/joda-time-2.9.9.jar").\
    getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:9000")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", 'true')
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", 'false')
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")


df = spark.read.json("s3a://mockdata/clickStream.json")

print(df.count())

# csv example

# spark.read.format('csv').options(header='true', inferSchema='true')
#     .load('s3a://sparkbyexamples/csv/zipcodes.csv')

# read multiple csv files
# df = spark.read.csv("s3 path1,s3 path2,s3 path3")

# read all files in a folder
# df = spark.read.csv("Folder path")

# write to s3 example

# df2.write
# .option("header","true")
# .csv("s3a://sparkbyexamples/csv/zipcodes")

# options example
# header, delimiter

# df2.write.options("header","true")
#  .csv("s3a://sparkbyexamples/csv/zipcodes")

# saving modes
# overwrite, append, ignore, errorifexists

# df2.write.mode(SaveMode.Overwrite).csv("s3a://sparkbyexamples/csv/zipcodes")







