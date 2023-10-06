from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

from pandas import *

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

python_df = session.create_dataframe(["a", "b", "c"])
pandas_df = python_df.to_pandas()