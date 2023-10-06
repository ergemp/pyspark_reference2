from snowflake.snowpark  import Session
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

# Get the list of the files in a stage.
# The collect() method causes this SQL statement to be executed.
session.sql("create or replace temp stage my_stage").collect()

# Prepend a return statement to return the collect() results in a Python worksheet
# [Row(status='Stage area MY_STAGE successfully created.')]

stage_files_df = session.sql("ls @my_stage").collect()
# Prepend a return statement to return the collect() results in a Python worksheet
# Resume the operation of a warehouse.
# Note that you must call the collect method to execute
# the SQL statement.
session.sql("alter warehouse if exists my_warehouse resume if suspended").collect()
# Prepend a return statement to return the collect() results in a Python worksheet
# [Row(status='Statement executed successfully.')]

# Set up a SQL statement to copy data from a stage to a table.
session.sql("copy into sample_product_data from @my_stage file_format=(type = csv)").collect()
# Prepend a return statement to return the collect() results in a Python worksheet
# [Row(status='Copy executed with 0 files processed.')]

#
#
#

# If you want to call methods to transform the DataFrame (e.g. filter, select, etc.),
# note that these methods work only if the underlying SQL statement is a SELECT statement.
# The transformation methods are not supported for other kinds of SQL statements.

df = session.sql("select id, parent_id from sample_product_data where id < 10")
# Because the underlying SQL statement for the DataFrame is a SELECT statement,
# you can call the filter method to transform this DataFrame.
results = df.filter(col("id") < 3).select(col("id")).collect()
print(results)
# Prepend a return statement to return the collect() results in a Python worksheet



