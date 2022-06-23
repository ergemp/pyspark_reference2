from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


df = spark.read.json('../data/clickStream.json')

df.groupby("event").count().show()
'''+------------+-----+                                                            
|       event|count|
+------------+-----+
|  newSession| 2467|
|boutiqueView| 2571|
| productView| 2403|
|orderSummary| 2559|
+------------+-----+
'''

print(df.filter('event="newSession"').count())
#2467

print(df.filter(df['event'] == 'newSession').count())
print(df.filter(df.event == 'newSession').count())
print(df.filter(df.event.like('newSession')).count())