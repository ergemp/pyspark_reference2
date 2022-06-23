from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [Row(name="James,,Smith",lang=["Java","Scala","C++"],state="CA"),
    Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NJ"),
    Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")]

rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())

collData=rdd.collect()

for row in collData:
    print(row.name + "," +str(row.lang))

data = [Row(name="James", prop=Row(hair="black", eye="blue")),
        Row(name="Ann", prop=Row(hair="grey", eye="black"))]
df = spark.createDataFrame(data)
df.printSchema()

