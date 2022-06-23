from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header", True).csv("../data/small_zipcodes.csv")

df.printSchema()

df.write.option("header", True) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("../output/tmp/zipcodes-state")

df.write.option("header", True) \
        .partitionBy("state", "city") \
        .mode("overwrite") \
        .csv("../output/tmp/zipcodes-state2")

# use repartition and partitionBby together

df.repartition(2) \
        .write.option("header", True) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("../output/tmp/zipcodes-state-more")

# control number of records for each partition

df.write.option("header", True) \
        .option("maxRecordsPerFile", 2) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("../output/tmp/zipcodes-state3")

# read a specific partition

dfSinglePart = spark.read.option("header", True).csv("../output/tmp/zipcodes-state/state=AL/city=SPRINGVILLE")
dfSinglePart.printSchema()
dfSinglePart.show()

parqDF = spark.read.option("header", True).csv("../output/tmp/zipcodes-state2")
parqDF.createOrReplaceTempView("ZIPCODE")

spark.sql("select * from ZIPCODE  where state='TX' and city = 'CINGULAR WIRELESS'").show()
