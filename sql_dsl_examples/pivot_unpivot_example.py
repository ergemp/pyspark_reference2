import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.getOrCreate()

data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
        ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
        ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
        ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product", "Amount", "Country"]

df = spark.createDataFrame(data=data, schema=columns)

df.printSchema()
df.show(truncate=False)

pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.printSchema()
pivotDF.show(truncate=False)

# performance improvement before spark 2.0
countries = ["USA", "China", "Canada", "Mexico"]
pivotDF = df.groupBy("Product").pivot("Country", countries).sum("Amount")
pivotDF.show(truncate=False)


'''
Another approach is to do two-phase aggregation. 
PySpark 2.0 uses this implementation 
in order to improve the performance Spark-13749
'''
pivotDF = df.groupBy("Product","Country") \
      .sum("Amount") \
      .groupBy("Product") \
      .pivot("Country") \
      .sum("sum(Amount)") \

pivotDF.show(truncate=False)

# unpivot examples

from pyspark.sql.functions import expr
unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"

unPivotDF = pivotDF.select("Product", expr(unpivotExpr)) \
    .where("Total is not null")

unPivotDF.show(truncate=False)
unPivotDF.show()




