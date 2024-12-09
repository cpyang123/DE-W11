# Databricks notebook source
import pandas as pd

# COMMAND ----------

# Load CSV data into a Spark DataFrame
# Step 1: Load CSV into Pandas DataFrame
csv_path = "https://raw.githubusercontent.com/cpyang123/house-price-pred/refs/heads/main/train.csv"
pandas_df = pd.read_csv(csv_path, nrows=1000)

# Step 2: Convert Pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(pandas_df)
# Create database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS peter_dev")

spark.sql("USE peter_dev")

# Save DataFrame as a Delta table in the specified database
spark_df.write.format("delta").mode("overwrite").saveAsTable("peter_dev.tbl_house_prices")

# Display the table to verify
display(spark.sql("SELECT * FROM peter_dev.tbl_house_prices LIMIT 100"))
