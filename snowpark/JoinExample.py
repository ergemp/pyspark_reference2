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

# Create two DataFrames to join
df_lhs = session.create_dataframe([["a", 1], ["b", 2]], schema=["key", "value1"])
df_rhs = session.create_dataframe([["a", 3], ["b", 4]], schema=["key", "value2"])
# Create a DataFrame that joins the two DataFrames
# on the column named "key".
df_lhs.join(df_rhs, df_lhs.col("key") == df_rhs.col("key")).select(df_lhs["key"].as_("key"), "value1", "value2").show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_lhs.join(df_rhs, df_lhs.col("key") == df_rhs.col("key")).select(df_lhs["key"].as_("key"), "value1", "value2")

#
#
#

# If both DataFrames have the same column to join on, you can use the following example syntax:

# Create two DataFrames to join
df_lhs = session.create_dataframe([["a", 1], ["b", 2]], schema=["key", "value1"])
df_rhs = session.create_dataframe([["a", 3], ["b", 4]], schema=["key", "value2"])
# If both dataframes have the same column "key", the following is more convenient.
df_lhs.join(df_rhs, ["key"]).show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_lhs.join(df_rhs, ["key"])

#
#
#

# You can also use the & operator to connect join expressions:

# Create two DataFrames to join
df_lhs = session.create_dataframe([["a", 1], ["b", 2]], schema=["key", "value1"])
df_rhs = session.create_dataframe([["a", 3], ["b", 4]], schema=["key", "value2"])

# Use & operator connect join expression. '|' and ~ are similar.
df_joined_multi_column = df_lhs.join(df_rhs, (df_lhs.col("key") == df_rhs.col("key")) & (df_lhs.col("value1") < df_rhs.col("value2"))).select(df_lhs["key"].as_("key"), "value1", "value2")
df_joined_multi_column.show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_joined_multi_column

#
#
#

# copy the DataFrame if you want to do a self-join
from copy import copy

# Create two DataFrames to join
df_lhs = session.create_dataframe([["a", 1], ["b", 2]], schema=["key", "value1"])
df_rhs = session.create_dataframe([["a", 3], ["b", 4]], schema=["key", "value2"])
df_lhs_copied = copy(df_lhs)
df_self_joined = df_lhs.join(df_lhs_copied, (df_lhs.col("key") == df_lhs_copied.col("key")) & (df_lhs.col("value1") == df_lhs_copied.col("value1")))

#
#
#

# When there are overlapping columns in the DataFrames,
# Snowpark prepends a randomly generated prefix to the columns in the join result:

# Create two DataFrames to join
df_lhs = session.create_dataframe([["a", 1], ["b", 2]], schema=["key", "value1"])
df_rhs = session.create_dataframe([["a", 3], ["b", 4]], schema=["key", "value2"])
df_lhs.join(df_rhs, df_lhs.col("key") == df_rhs.col("key")).show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_lhs.join(df_rhs, df_lhs.col("key") == df_rhs.col("key"))

#
#
#

# You can rename the overlapping columns using Column.alias:

# Create two DataFrames to join
df_lhs = session.create_dataframe([["a", 1], ["b", 2]], schema=["key", "value1"])
df_rhs = session.create_dataframe([["a", 3], ["b", 4]], schema=["key", "value2"])
df_lhs.join(df_rhs, df_lhs.col("key") == df_rhs.col("key")).select(df_lhs["key"].alias("key1"), df_rhs["key"].alias("key2"), "value1", "value2").show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_lhs.join(df_rhs, df_lhs.col("key") == df_rhs.col("key")).select(df_lhs["key"].alias("key1"), df_rhs["key"].alias("key2"), "value1", "value2")

# To avoid random prefixes, you can also specify a suffix to append to the overlapping columns:
# Create two DataFrames to join
df_lhs = session.create_dataframe([["a", 1], ["b", 2]], schema=["key", "value1"])
df_rhs = session.create_dataframe([["a", 3], ["b", 4]], schema=["key", "value2"])
df_lhs.join(df_rhs, df_lhs.col("key") == df_rhs.col("key"), lsuffix="_left", rsuffix="_right").show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_lhs.join(df_rhs, df_lhs.col("key") == df_rhs.col("key"), lsuffix="_left", rsuffix="_right")



# If you need to join a table with itself on different columns,
# you cannot perform the self-join with a single DataFrame.

# Create a DataFrame object for the "sample_product_data" table for the left-hand side of the join.
df_lhs = session.table("sample_product_data")
# Clone the DataFrame object to use as the right-hand side of the join.
df_rhs = copy(df_lhs)

# Create a DataFrame that joins the two DataFrames
# for the "sample_product_data" table on the
# "id" and "parent_id" columns.
df_joined = df_lhs.join(df_rhs, df_lhs.col("id") == df_rhs.col("parent_id"))
print(df_joined.count())

#
#
#

# You can use Column objects with the join method to define a join condition:

dfX = session.create_dataframe([[1], [2]], schema=["a_in_X"])
dfY = session.create_dataframe([[1], [3]], schema=["b_in_Y"])
# Specify the equivalent of "X JOIN Y on X.a_in_X = Y.b_in_Y"
# in a SQL SELECT statement.
df_joined = dfX.join(dfY, col("a_in_X") == col("b_in_Y")).select(dfX["a_in_X"].alias("the_joined_column"))
df_joined.show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_joined

#
#
#

# When referring to columns in two different DataFrame objects that have the same name
# (for example, joining the DataFrames on that column),
# you can use the DataFrame.col method in one DataFrame object
# to refer to a column in that object (for example, df1.col("name") and df2.col("name")).

# Create two DataFrames to join
df_lhs = session.create_dataframe([["a", 1], ["b", 2]], schema=["key", "value"])
df_rhs = session.create_dataframe([["a", 3], ["b", 4]], schema=["key", "value"])
# Create a DataFrame that joins two other DataFrames (df_lhs and df_rhs).
# Use the DataFrame.col method to refer to the columns used in the join.
df_joined = df_lhs.join(df_rhs, df_lhs.col("key") == df_rhs.col("key")).select(df_lhs.col("key").as_("key"), df_lhs.col("value").as_("L"), df_rhs.col("value").as_("R"))
df_joined.show()
# To return the DataFrame as a table in a Python worksheet use return instead of show()
# return df_joined







