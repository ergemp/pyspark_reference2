from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data=[("James","Bond","100",None),
      ("Ann","Varsa","200",'F'),
      ("Tom Cruise","XXX","400",''),
      ("Tom Brand",None,"400",'M')]

columns=["fname", "lname", "id", "gender"]

df=spark.createDataFrame(data, columns)

# alias example

from pyspark.sql.functions import expr

df.select(df.fname.alias("first_name"), \
          df.lname.alias("last_name")
   ).show()

#Another example
df.select(expr(" fname ||','|| lname").alias("fullName") \
   ).show()

# asc, desc examples

df.sort(df.fname.asc()).show()
df.sort(df.fname.desc()).show()

# cast examples

df.select(df.fname, df.id.cast("int")).printSchema()

# between example

df.filter(df.id.between(100,300)).show()

# contains example

df.filter(df.fname.contains("Cruise")).show()

# startswith, endswith example

df.filter(df.fname.startswith("T")).show()
df.filter(df.fname.endswith("Cruise")).show()

# isNull, isNotNull example

df.filter(df.lname.isNull()).show()
df.filter(df.lname.isNotNull()).show()

# like example

df.select(df.fname,df.lname,df.id) \
      .filter(df.fname.like("%es")).show()

# substr example

df.select(df.fname.substr(1,2).alias("substr")).show()

# when otherwise example

from pyspark.sql.functions import when

df.select(df.fname,df.lname, \
               when(df.gender=="M","Male") \
              .when(df.gender=="F","Female") \
              .when(df.gender==None , None) \
              .when(df.gender=="",  None)
              .otherwise(df.gender) \
              .alias("new_gender") \
    ).show()

# isin example

li=["100","200"]

df.select(df.fname,df.lname,df.id) \
  .filter(df.id.isin(li)) \
  .show()

# getField example

from pyspark.sql.types import StructType,StructField,StringType,ArrayType,MapType

data=[(("James","Bond"),["Java","C#"],{'hair':'black','eye':'brown'}),
      (("Ann","Varsa"),[".NET","Python"],{'hair':'brown','eye':'black'}),
      (("Tom Cruise",""),["Python","Scala"],{'hair':'red','eye':'grey'}),
      (("Tom Brand",None),["Perl","Ruby"],{'hair':'black','eye':'blue'})]

schema = StructType([
        StructField('name', StructType([
            StructField('fname', StringType(), True),
            StructField('lname', StringType(), True)])),
        StructField('languages', ArrayType(StringType()),True),
        StructField('properties', MapType(StringType(),StringType()),True)
     ])

df = spark.createDataFrame(data,schema)
df.printSchema()

#getField from MapType
df.select(df.properties.getField("hair")).show()
df.select(df.properties.hair).show()

#getField from Struct
df.select(df.name.getField("fname")).show()
df.select(df.name.fname).show()
df.select("name.fname").show()

#getItem() used with ArrayType
df.select(df.languages.getItem(1)).show()

#getItem() used with MapType
df.select(df.properties.getItem("hair")).show()