from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark = SparkSession.builder.appName('date_time_functions_example').getOrCreate()

# Most of all these functions accept input as, Date type, Timestamp type, or String.
# If a String used, it should be in a default format that can be cast to date.

# DateType default format is yyyy-MM-dd
# TimestampType default format is yyyy-MM-dd HH:mm:ss.SSSS
# Returns null if the input is a string that can not be cast to Date or Timestamp.

data = [["1", "2020-02-01"], ["2", "2019-03-01"], ["3", "2021-03-01"]]
df = spark.createDataFrame(data, ["id", "input"])
df.show()

# current_date()

df.select(current_date().alias("current_date")).show(1)
df.withColumn("current", lit(current_date())).show()

# date_format()
df.select(col("input"), date_format(col("input"), "MM-dd-yyyy").alias("date_format")).show()

# to_date()
df.select(col("input"), to_date(col("input"), "yyyy-MM-dd").alias("to_date")).show()
df.select(df["input"], to_date(col("input"), "yyyy-MM-dd").alias("to_date")).show()

# datediff()
df.select(col("input"), datediff(current_date(), col("input")).alias("datediff")).show()

# months_between()
df.select(col("input"), months_between(current_date(), col("input")).alias("months_between")).show()

# trunc()
'''
Returns date truncated to the unit specified by the format.
For example, `trunc("2018-11-19 12:01:19", "year")` returns 2018-01-01
format: 'year', 'yyyy', 'yy' to truncate by year,
'month', 'mon', 'mm' to truncate by month
'''
df.select(col("input"),
    trunc(col("input"), "Month").alias("Month_Trunc"),
    trunc(col("input"), "Year").alias("Month_Year"),
    trunc(col("input"), "Month").alias("Month_Trunc")
   ).show()

# date_trunc()
'''
Returns timestamp truncated to the unit specified by the format.
For example, `date_trunc("year", "2018-11-19 12:01:19")` returns 2018-01-01 00:00:00
format: 'year', 'yyyy', 'yy' to truncate by year,
'month', 'mon', 'mm' to truncate by month,
'day', 'dd' to truncate by day,
Other options are: 'second', 'minute', 'hour', 'week', 'month', 'quarter'
'''

# add_months() , date_add(), date_sub()
df.select(col("input"),
    add_months(col("input"),3).alias("add_months"),
    add_months(col("input"),-3).alias("sub_months"),
    date_add(col("input"),4).alias("date_add"),
    date_sub(col("input"),4).alias("date_sub")
  ).show()

# year(), month(), month(), next_day(), last_day(), weekofyear()
df.select(col("input"),
     year(col("input")).alias("year"),
     month(col("input")).alias("month"),
     next_day(col("input"), "Sunday").alias("next_day"),
     last_day(col("input")).alias("ladt_day"),
     weekofyear(col("input")).alias("weekofyear")
  ).show()

# dayofweek(), dayofmonth(), dayofyear()
df.select(col("input"),
     dayofweek(col("input")).alias("dayofweek"),
     dayofmonth(col("input")).alias("dayofmonth"),
     dayofyear(col("input")).alias("dayofyear"),
  ).show()

#
# timestamp functions
#

data = [["1", "02-01-2020 11 01 19 06"], ["2", "03-01-2019 12 01 19 406"], ["3", "03-01-2021 12 01 19 406"]]
df2 = spark.createDataFrame(data, ["id", "input"])
df2.show(truncate=False)

# current_timestamp()
df2.select(current_timestamp().alias("current_timestamp")).show(1, truncate=False)

# to_timestamp()
df2.select(col("input"), to_timestamp(col("input"), "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp")).show(truncate=False)

# hour(), minute() and second()
data = [["1", "2020-02-01 11:01:19.06"], ["2", "2019-03-01 12:01:19.406"], ["3", "2021-03-01 12:01:19.406"]]
df3 = spark.createDataFrame(data, ["id", "input"])

df3.select(col("input"),
    hour(col("input")).alias("hour"),
    minute(col("input")).alias("minute"),
    second(col("input")).alias("second")
  ).show(truncate=False)

# unix_timestamp(), from_unixtime()
df3\
    .withColumn("unix_time", unix_timestamp())\
    .withColumn("unix_time_conv", unix_timestamp(to_timestamp(col("input"))))\
    .withColumn("timestamp_conv", from_unixtime(unix_timestamp(to_timestamp(col("input")))))\
    .show(truncate=False)
