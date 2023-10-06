from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

connection_parameters = {
  "account": "pwskioq-xi02455",
  "user": "ergemp",
  "password": "Pass13word",
  "role": "accountadmin",  # optional
  "warehouse": "sf_tuts_wh",  # optional
  "database": "sf_tuts",  # optional
  "schema": "public",  # optional
}

session = Session.builder.configs(connection_parameters).create()

df = session.table("car_sales")
df.select(col("src")["dealership"]).show()

df.select(df["src"]["salesperson"]["name"]).show()

df.select(df["src"]["vehicle"][0]).show()
df.select(df["src"]["vehicle"][0]["price"]).show()

#
#
#

# you can use get, get_ignore_case, or get_path functions
# if the field name or elements in the path are irregular

from snowflake.snowpark.functions import get, get_path, lit

df.select(get(col("src"), lit("dealership"))).show()
df.select(col("src")["dealership"]).show()

df.select(get_path(col("src"), lit("vehicle[0].make"))).show()
df.select(col("src")["vehicle"][0]["make"]).show()

#
#
#

# Explicitly Casting Values in Semi-Structured Data¶

from snowflake.snowpark.types import *

df.select(col("src")["salesperson"]["id"]).show()
df.select(col("src")["salesperson"]["id"].cast(StringType())).show()

#
#
#

# Flattening an Array of Objects into Rows¶

df = session.table("car_sales")
df.join_table_function("flatten", col("src")["customer"]).show()
df.join_table_function("flatten", col("src")["customer"]).select(col("value")["name"], col("value")["address"]).show()
df.join_table_function("flatten", col("src")["customer"]).select(col("value")["name"].cast(StringType()).as_("Customer Name"), col("value")["address"].cast(StringType()).as_("Customer Address")).show()

