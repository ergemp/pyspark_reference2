from functions import get_english_name, get_start_year, get_trend
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

print(get_english_name("Greenfinch (Chloris chloris)"));
print (get_trend(-1.15));
print (get_start_year("(1995)"));

get_english_name_udf = udf(lambda z: get_english_name(z), StringType()).asNondeterministic()

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

data = [('James ( ttt )','','Smith','1991-04-01','M',3000)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

df = spark.createDataFrame(data=data, schema=columns)
df.createOrReplaceTempView("df")
#df.selectExpr(get_english_name_udf(col("firstname"))).show()

#df.select(get_english_name_udf(col("firstname")).alias("firstname")).show()
df.withColumn("new_first_name", get_english_name_udf(col("firstname"))).show()


