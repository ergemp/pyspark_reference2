import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.dataframe_reader import *
from snowflake.snowpark.functions import *

def main(session: snowpark.Session):

  inputTableName = "snowflake.account_usage.task_history"
  outputTableName = "aggregate_task_history"

  df = session.table(inputTableName)
  df.filter(col("STATE") != "SKIPPED")\
    .group_by(("SCHEDULED_TIME"), "STATE").count()\
    .write.mode("overwrite").save_as_table(outputTableName)
  return outputTableName + " table successfully written"