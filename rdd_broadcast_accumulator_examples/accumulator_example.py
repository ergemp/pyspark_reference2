'''
The PySpark Accumulator is a shared variable that is used with RDD and DataFrame
to perform sum and counter operations similar to Map-reduce counters.
These variables are shared by all executors to update and add information through aggregation or computative operations.

Accumulators are write-only and initialize once variables where only tasks that are running on workers
are allowed to update and updates from the workers get propagated automatically to the driver program.
But, only the driver program is allowed to access the Accumulator variable using the value property.

sparkContext.accumulator() is used to define accumulator variables.
add() function is used to add/update a value in accumulator
value property on the accumulator variable is used to retrieve the value from the accumulator.
'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("accumulator").getOrCreate()

# Note that, In this example, rdd.foreach() is executed on workers and
# accum.value is called from PySpark driver program.

accum = spark.sparkContext.accumulator(0)
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd.foreach(lambda x: accum.add(x))
print(accum.value)

accuSum = spark.sparkContext.accumulator(0)


def countFun(x):
    global accuSum
    accuSum += x


rdd.foreach(countFun)
print(accuSum.value)

accumCount = spark.sparkContext.accumulator(0)
rdd2 = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd2.foreach(lambda x: accumCount.add(1))

print(accumCount.value)

