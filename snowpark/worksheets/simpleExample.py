import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
  tableName = "range_table"
  df_range = session.range(1, 10, 2).to_df('a')
  df_range.write.mode("overwrite").save_as_table(tableName)
  return tableName + " table successfully created"