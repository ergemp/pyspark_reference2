from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [('James','Smith','M',30),
        ('Anna','Rose','F',41),
        ('Robert','Williams','M',62)]

columns = ["firstname", "lastname", "gender", "salary"]

df = spark.createDataFrame(data=data, schema = columns)

df.show()

# Foreach example

def f(x):
    print(x)

df.foreach(f)

# Another example

df.foreach(lambda x:
    print("Data ==>"+x["firstname"]+","+x["lastname"]+","+x["gender"]+","+str(x["salary"]*2))
    )