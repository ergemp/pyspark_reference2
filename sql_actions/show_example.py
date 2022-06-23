from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("create_df_from_csv_file").getOrCreate()

df = spark.read.option("delimiter", ",").option("inferSchema", True).option("header", True).csv("../data/category.csv")

df.show()
#df.show(n=20, truncate=True, vertical=False) # by default
df.show(n=50, truncate=False, vertical=True)



