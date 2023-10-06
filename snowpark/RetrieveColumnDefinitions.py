from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Import the StructType
from snowflake.snowpark.types import *

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


# Get the StructType object that describes the columns in the
# underlying rowset.
table_schema = session.table("sample_product_data").schema
table_schema
StructType([StructField('ID', LongType(), nullable=True), StructField('PARENT_ID', LongType(), nullable=True), StructField('CATEGORY_ID', LongType(), nullable=True), StructField('NAME', StringType(), nullable=True), StructField('SERIAL_NUMBER', StringType(), nullable=True), StructField('KEY', LongType(), nullable=True), StructField('"3rd"', LongType(), nullable=True)])

# Create a DataFrame containing the "id" and "3rd" columns.
df_selected_columns = session.table("sample_product_data").select(col("id"), col("3rd"))

# Print out the names of the columns in the schema. This prints out:
# This prints List["ID", "\"3rd\""]
print(df_selected_columns.schema.names)
# ['ID', '"3rd"']

