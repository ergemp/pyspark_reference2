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

# Define a DataFrame
df_products = session.table("sample_product_data")
# Define a View name
view_name = "my_view"
# Create the view
df_products.create_or_replace_view(f"{view_name}")


# df.create_or_replace_view(f"{database}.{schema}.{view_name}")
# [Row(status='View MY_VIEW successfully created.')]