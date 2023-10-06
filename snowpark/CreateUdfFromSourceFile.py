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

# mod5() in that file has type hints
mod5_udf = session.udf.register_from_file(
     file_path="test_udf_file.py",
     func_name="mod5",
)

session.range(1, 8, 2).select(mod5_udf("id")).to_df("col1").collect()
# [Row(COL1=1), Row(COL1=3), Row(COL1=0), Row(COL1=2)]

# You can also upload the file to a stage location, then use it to create the UDF.

# suppose you have uploaded test_udf_file.py to stage location @mystage.

'''
mod5_udf = session.udf.register_from_file(
     file_path="@mystage/test_udf_file.py",
     func_name="mod5",
     return_type=IntegerType(),
     input_types=[IntegerType()],
 )
'''

