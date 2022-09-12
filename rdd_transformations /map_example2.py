from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

data = ["Project", "Gutenberg’s", "Alice’s", "Adventures",
        "in", "Wonderland", "Project", "Gutenberg’s", "Adventures",
        "in", "Wonderland", "Project", "Gutenberg’s"]

rdd = spark.sparkContext.parallelize(data)

# map example with rdd

rdd2 = rdd.map(lambda x: (x, 1))

for element in rdd2.collect():
    print(element)

# map example with dataframe

data = [('James','Smith','M',30),
        ('Anna','Rose','F',41),
        ('Robert','Williams','M',62)]

columns = ["firstname", "lastname", "gender", "salary"]

df = spark.createDataFrame(data=data, schema = columns)

df.show()


# Refering columns by index.
rdd2 = df.rdd.map(lambda x: (x[0]+","+x[1], x[2], x[3]*2))
df2 = rdd2.toDF(["name", "gender", "new_salary"])

df2.show()

# Referring Column Names
rdd2 = df.rdd.map(lambda x: (x["firstname"]+","+x["lastname"], x["gender"], x["salary"]*2))

# Referring Column Names
rdd2 = df.rdd.map(lambda x: (x.firstname+","+x.lastname, x.gender, x.salary*2))

# ###
# By Calling function
# ###

def func1(x):
    firstName = x.firstname
    lastName = x.lastname
    name = firstName+","+lastName
    gender = x.gender.lower()
    salary = x.salary*2
    return name, gender, salary


rdd2 = df.rdd.map(lambda x: func1(x)).toDF().show()

columns = ["name", "gender", "salary"]
rdd2 = df.rdd.map(lambda x: func1(x)).toDF(columns).show()







