from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("create_df_from_csv_file").getOrCreate()

df = spark.read.text("data/category.csv")
df.printSchema()



