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

from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.functions import udf

# add_one = udf(lambda x: x+1, return_type=IntegerType(), input_types=[IntegerType()])

# or register function
@udf(name="add_one", is_permanent=False, replace=True)
def add_one(x: int) -> int:
  return x - 1

df = session.create_dataframe([[1, 2], [3, 4]]).to_df("a", "b")
df.select(add_one("a"), add_one("b")).collect()

# if udf registered then;
# You can also call the UDF using SQL:
session.sql("select add_one(1)").collect()



