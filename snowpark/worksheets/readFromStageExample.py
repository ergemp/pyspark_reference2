import snowflake.snowpark as snowpark
from snowflake.snowpark.types import *

schema_for_file = StructType([
  StructField("name", StringType()),
  StructField("role", StringType())
])

fileLocation = "@DB1.PUBLIC.FILES/data_0_0_0.csv.gz"
outputTableName = "employees"

def main(session: snowpark.Session):
  df_reader = session.read.schema(schema_for_file)
  df = df_reader.csv(fileLocation)
  df.write.mode("overwrite").save_as_table(outputTableName)

  return outputTableName + " table successfully written from stage"