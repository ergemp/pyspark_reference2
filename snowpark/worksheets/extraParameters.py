# The Snowpark package is required for Python Worksheets.
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

# Add parameters with optional type hints to the main handler function
def main(session: snowpark.Session, language: str):
  # Your code goes here, inside the "main" handler.
  table_name = 'information_schema.packages'
  dataFrame = session.table(table_name).filter(col("language") == language)

  # Print a sample of the dataFrame to standard output
  dataFrame.show()

  # The return value appears in the Results tab
  return dataFrame

# Add a second function to supply a value for the language parameter to validate that your main handler function runs.
def test_language(session: snowpark.Session):
  return main(session, 'java')