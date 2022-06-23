from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "lib/postgresql-42.2.24.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "person") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()
df.show(truncate=False)

#
# write to jdbc example
#

# pyspark.sql.utils.AnalysisException: Table or view 'public.person2' already exists. SaveMode: ErrorIfExists.
df.write \
    .mode("overwrite") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "public.person2") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .save()

# to create column types
# .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")


