from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

emp = [ (1,"Smith",-1,"2018","10","M",3000), \
        (2,"Rose",1,"2010","20","M",4000), \
        (3,"Williams",1,"2010","10","M",1000), \
        (4,"Jones",2,"2005","10","F",2000), \
        (5,"Brown",2,"2010","40","",-1), \
        (6,"Brown",2,"2010","50","",-1) ]
empColumns = ["emp_id", "name", "superior_emp_id", "year_joined", "emp_dept_id", "gender", "salary"]

empDF = spark.createDataFrame(data=emp, schema=empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
        ("Marketing",20), \
        ("Sales",30), \
        ("IT",40) ]
deptColumns = ["dept_name", "dept_id"]

deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# inner join example

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "inner").show(truncate=False)

# Full Outer Join
# Outer a.k.a full, fullouter

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "outer").show(truncate=False)
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "full").show(truncate=False)
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "fullouter").show(truncate=False)

# Left Outer Join
# Left a.k.a Leftouter

empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], "left").show(truncate=False)
empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], "leftouter").show(truncate=False)

# Right Outer Join
# Right a.k.a Rightouter

empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], "right").show(truncate=False)
empDF.join(deptDF, empDF["emp_dept_id"] == deptDF["dept_id"], "rightouter").show(truncate=False)

# self join example

empDF.alias("emp1").join(empDF.alias("emp2"), \
    col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner") \
    .select(col("emp1.emp_id"), col("emp1.name"), \
        col("emp2.emp_id").alias("superior_emp_id"), \
        col("emp2.name").alias("superior_emp_name")) \
   .show(truncate=False)

# join with raw sql

empDF.createOrReplaceTempView("EMP")
deptDF.createOrReplaceTempView("DEPT")

joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id") \
  .show(truncate=False)

joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id") \
  .show(truncate=False)

# multiple join example

'''
df1.join(df2,df1.id1 == df2.id2,"inner") \
   .join(df3,df1.id1 == df3.id3,"inner")
'''