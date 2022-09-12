'''
Broadcast variables are read-only shared variables that are cached and available on all nodes in a cluster
in-order to access or use by the tasks

You should be creating and using broadcast variables for data that shared across multiple stages and tasks.

broadcast variables are not sent to executors with sc.broadcast(variable) call instead,
they will be sent to executors when they are first used.
'''

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

states = {"NY": "New York", "CA": "California", "FL": "Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data =  [("James","Smith","USA","CA"),
         ("Michael","Rose","USA","NY"),
         ("Robert","Williams","USA","CA"),
         ("Maria","Jones","USA","FL")
        ]

rdd = spark.sparkContext.parallelize(data)


def state_convert(code):
    return broadcastStates.value[code]


result = rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).collect()
print(result)


