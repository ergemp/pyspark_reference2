from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()

df.select("salary").show()

#  Change DataType using PySpark withColumn()
df.withColumn("salary", df["salary"].cast("Integer")).show()

# Since we used round () brackets, pyspark thinks that we’re attempting to call the DataFrame as a function.
# df.withColumn("salary", df("salary").cast("Integer")).show()

# Update The Value of an Existing Column
df.withColumn("salary", col("salary") * 100).show()

# Create a Column from an Existing

df.withColumn("CopiedColumn", col("salary") * -1).show()

# Add a New Column using withColumn()

df.withColumn("Country", lit("USA")).show()
df.withColumn("Country", lit("USA")) \
  .withColumn("anotherColumn", lit("anotherValue")) \
  .show()

# Rename Column Name
df.withColumnRenamed("gender", "sex").show(truncate=False)

# Drop a Column

df.drop("salary").show()
