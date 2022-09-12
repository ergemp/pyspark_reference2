from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

'''
df.write.parquet("output/people.parquet") 
parDF1 = spark.read.parquet("data/people.parquet")
'''

data = [("James ", "", "Smith", "36636", "M", 3000),
        ("Michael ", "Rose", "", "40288", "M", 4000),
        ("Robert ", "", "Williams", "42114", "M", 4000),
        ("Maria ", "Anne", "Jones", "39192", "F", 4000),
        ("Jen", "Mary", "Brown", "", "F", -1)]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

df = spark.createDataFrame(data, columns)

# df.write.parquet("output/people.parquet")
# df.write.mode('append').parquet("output/people.parquet")
df.write.mode('overwrite').parquet("output/people.parquet")

pDf = spark.read.parquet("output/people.parquet")

pDf.createOrReplaceTempView("ParquetTable")
pSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")

pSQL.show()

# create table on parquet file

spark.sql("CREATE TEMPORARY VIEW PERSON USING parquet OPTIONS (path \"output/people.parquet\")")
spark.sql("SELECT * FROM PERSON").show()


# write partitionedBy
df.write.partitionBy("gender", "salary").mode("overwrite").parquet("output/people2.parquet")

# reading from partitionedBy

parDF2 = spark.read.parquet("output/people2.parquet/gender=M")
parDF2.show(truncate=False)

# creating table on partitioned parquet

spark.sql("CREATE TEMPORARY VIEW PERSON2 USING parquet OPTIONS (path \"output/people2.parquet/gender=F\")")
spark.sql("SELECT * FROM PERSON2").show()








