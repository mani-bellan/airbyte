import airbyte as ab
import duckdb

#Create the source connection and read the data
source = ab.get_source(
    "source-faker",
    config={"count": 5_000},
    install_if_missing=True
)
source.check()
source.select_all_streams()
result = source.read()

# Load the results from streams to pandas dataframes
products_df = result["products"].to_pandas()
users_df = result["users"].to_pandas()
purchases_df = result["purchases"].to_pandas()

#Write the dataframe data to duckdb tables
duckdb.sql("CREATE TABLE product_dim AS SELECT * FROM products_df")
duckdb.sql("CREATE TABLE user_dim AS SELECT * FROM users_df")
duckdb.sql("CREATE TABLE purchases_fct AS SELECT * FROM purchases_df")

# insert data into the tables from the data frames
duckdb.sql("INSERT INTO product_dim SELECT * FROM products_df")
duckdb.sql("INSERT INTO user_dim SELECT * FROM users_df")
duckdb.sql("INSERT INTO purchases_fct SELECT * FROM purchases_df")

# show data from duck db
duckdb.sql("SELECT * FROM product_dim limit 3").show()
duckdb.sql("SELECT * FROM user_dim limit 3").show()
duckdb.sql("SELECT * FROM purchases_fct limit 3").show()


