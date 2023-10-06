from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType
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
# Create DataFrames from data in a stage.
df_json = session.read.json("@my_stage2/data1.json")
df_catalog = session.read.schema(StructType([StructField("name", StringType()), StructField("age", IntegerType())])).csv("@stage/some_dir")