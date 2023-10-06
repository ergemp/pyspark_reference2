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

df1 = session.create_dataframe([1, 2, 3, 4]).to_df("a")
df1.show()

# Create a DataFrame with 4 columns, "a", "b", "c" and "d".
df2 = session.create_dataframe([[1, 2, 3, 4]], schema=["a", "b", "c", "d"])
df2.show()

# Create another DataFrame with 4 columns, "a", "b", "c" and "d".
from snowflake.snowpark import Row
df3 = session.create_dataframe([Row(a=1, b=2, c=3, d=4)])
df3.show()

# Create a DataFrame and specify a schema
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
df4 = session.create_dataframe([[1, "snow"], [3, "flake"]], schema)
df4.show()

# Create a DataFrame from a range
# The DataFrame contains rows with values 1, 3, 5, 7, and 9 respectively.
df_range = session.range(1, 10, 2).to_df("a")
df_range.show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_range



