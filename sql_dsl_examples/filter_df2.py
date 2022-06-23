from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType


spark = SparkSession.builder.getOrCreate()

data = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df.printSchema()
df.show(truncate=False)

# filtering with different options

df.filter(df.state == "OH").show(truncate=False)
df.filter(df["state"] == "OH").show(truncate=False)
df.filter(df["state"].eqNullSafe("OH")).show(truncate=False)
df.filter("state == 'OH'").show(truncate=False )

# filtering on nested struct types

df.filter(df.name.lastname == "Williams").show(truncate=False)

# multiple filter options

df.filter( (df.state  == "OH") & (df.gender  == "M") ).show(truncate=False)

# filter based on list values

li=["OH","CA","DE"]
df.filter(df.state.isin(li)).show()

# Using startswith

df.filter(df.state.startswith("N")).show()

# using endswith

df.filter(df.state.endswith("H")).show()

# contains

df.filter(df.state.contains("H")).show()

# filter based on array columns

from pyspark.sql.functions import array_contains

df.filter(array_contains(df.languages, "Java")).show(truncate=False)

# filter with like and rlike

data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
         (4,"Rames Rose"),(5,"Rames rose")]

df2 = spark.createDataFrame(data = data2, schema = ["id","name"])

# like - SQL LIKE pattern
df2.filter(df2.name.like("%rose%")).show()

# rlike - SQL RLIKE pattern (LIKE with Regex)
# This check case insensitive
df2.filter(df2.name.rlike("(?i)^*rose$")).show()
