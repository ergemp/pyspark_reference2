from snowflake.snowpark import Session

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

# Import the col function from the functions module.
# Python worksheets import this function by default
from snowflake.snowpark.functions import col

# Create a DataFrame for the rows with the ID 1
# in the "sample_product_data" table.

# This example uses the == operator of the Column object to perform an
# equality check.
df = session.table("sample_product_data").filter(col("id") == 1)
df.show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df

#
#
#

df = session.table("sample_product_data").select(col("id"), col("name"), col("serial_number"))
df.show()

#
#
#

df_product_info = session.table("sample_product_data")
df1 = df_product_info.select(df_product_info["id"], df_product_info["name"], df_product_info["serial_number"])
df2 = df_product_info.select(df_product_info.id, df_product_info.name, df_product_info.serial_number)
df3 = df_product_info.select("id", "name", "serial_number")

#
#
#

# Import the col function from the functions module.
from snowflake.snowpark.functions import col

df_product_info = session.table("sample_product_data").select(col("id"), col("name"))
df_product_info.show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_product_info

#
#
#

df = session.create_dataframe([[1, 3], [2, 10]], schema=["a", "b"])
# Specify the equivalent of "WHERE a + b < 10"
# in a SQL SELECT statement.
df_filtered = df.filter((col("a") + col("b")) < 10)
df_filtered.show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_filtered

#
#
#

# use Column objects with the select method to define an alias:

df = session.create_dataframe([[1, 3], [2, 10]], schema=["a", "b"])
# Specify the equivalent of "SELECT b * 10 AS c"
# in a SQL SELECT statement.
df_selected = df.select((col("b") * 10).as_("c"))
df_selected.show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_selected

#
#
#

# Using Literals as Column Objects
# Import for the lit and col functions.
from snowflake.snowpark.functions import col, lit

# Show the first 10 rows in which num_items is greater than 5.
# Use `lit(5)` to create a Column object for the literal 5.
df_filtered = df.filter(col("num_items") > lit(5))

#
#
#

# Casting a Column Object to a Specific Type

# Import for the lit function.
from snowflake.snowpark.functions import lit

# Import for the DecimalType class.
from snowflake.snowpark.types import DecimalType

decimal_value = lit(0.05).cast(DecimalType(5, 2))

#
#
#

# Chaining Method Calls¶

df_product_info = session.table("sample_product_data").filter(col("id") == 1).select(col("name"), col("serial_number"))
df_product_info.show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_product_info




