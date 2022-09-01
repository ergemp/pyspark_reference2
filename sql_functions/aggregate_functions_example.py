from pyspark.sql import SparkSession
# from pyspark.sql.functions import approx_count_distinct, avg, collect_list, collect_set, \
#   countDistinct, count, first, last

from pyspark.sql.functions import *


spark = SparkSession.builder.appName('approx_count_distinct_example').getOrCreate()

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]

schema = ["employee_name", "department", "salary"]

df = spark.createDataFrame(data=simpleData, schema=schema)
df.printSchema()
df.show(truncate=False)

# approx_count_distinct() function returns the count of distinct items in a group.
print("approx_count_distinct: " + str(df.select(approx_count_distinct("salary")).collect()[0][0]))

# avg() function returns the average of values in the input column.
print("avg: " + str(df.select(avg("salary")).collect()[0][0]))

# collect_list() function returns all values from an input column with duplicates.
df.select(collect_list("salary")).show(truncate=False)

# collect_set() function returns all values from an input column with duplicate values eliminated.
df.select(collect_set("salary")).show(truncate=False)

# countDistinct() function returns the number of distinct elements in a columns
df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)
print("Distinct Count of Department & Salary: " + str(df2.collect()[0][0]))

# count() function returns number of elements in a column.
print("count: "+str(df.select(count("salary")).collect()[0]))

# first() function returns the first element in a column
# when ignoreNulls is set to true, it returns the first non-null element.

df.select(first("salary", ignorenulls=True)).show(truncate=False)
df.select(first("salary", ignorenulls=False)).show(truncate=False)
df.select(first("salary")).show(truncate=False)

# last() function returns the last element in a column.
# when ignoreNulls is set to true, it returns the last non-null element.

df.select(last("salary")).show(truncate=False)

# Kurtosis is a measure of whether the data are heavy-tailed or light-tailed relative to a normal distribution.
# That is, data sets with high kurtosis tend to have heavy tails, or outliers.
# Data sets with low kurtosis tend to have light tails, or lack of outliers.
# A uniform distribution would be the extreme case.

df.select(kurtosis("salary")).show(truncate=False)

# max() function returns the maximum value in a column.
df.select(max("salary")).show(truncate=False)

# min() function
df.select(min("salary")).show(truncate=False)

# mean() function returns the average of the values in a column. Alias for Avg
df.select(mean("salary")).show(truncate=False)

# skewness tells us about the direction of outliers.
# You can see that our distribution is positively skewed and
# most of the outliers are present on the right side of the distribution.
# Note: The skewness does not tell us about the number of outliers. It only tells us the direction

df.select(skewness("salary")).show(truncate=False)

# stddev() alias for stddev_samp.
# stddev_samp() function returns the sample standard deviation of values in a column.
# stddev_pop() function returns the population standard deviation of the values in a column.

df.select(stddev("salary"), stddev_samp("salary"), stddev_pop("salary")).show(truncate=False)

# sum() function Returns the sum of all values in a column.
df.select(sum("salary")).show(truncate=False)

# sumDistinct() function returns the sum of all distinct values in a column.
df.select(sumDistinct("salary")).show(truncate=False)


# variance() alias for var_samp
# var_samp() function returns the unbiased variance of the values in a column.
# var_pop() function returns the population variance of the values in a column.

# Subtract the mean from each data value and square the result.
# Find the sum of all the squared differences.
# The sum of squares is all the squared differences added together.
# Calculate the variance.

# The variance is a measure of variability.
# It is calculated by taking the average of squared deviations from the mean.
# Variance tells you the degree of spread in your data set.
# The more spread the data, the larger the variance is in relation to the mean.

df.select(variance("salary"), var_samp("salary"), var_pop("salary")).show(truncate=False)






