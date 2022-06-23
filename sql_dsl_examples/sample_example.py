'''
sample(withReplacement, fraction, seed=None)

fraction
Fraction of rows to generate, range [0.0, 1.0].
Note that it doesn’t guarantee to provide the exact number of the fraction of records.

seed
Seed for sampling (default a random seed). Used to reproduce the same random sampling.

withReplacement
Sample with replacement or not (default False).
'''

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

df = spark.range(100)

print(df.sample(0.06).collect())

# Using seed to reproduce the same Samples

print(df.sample(0.1,123).collect())

print(df.sample(0.1,123).collect())

print(df.sample(0.1,456).collect())


