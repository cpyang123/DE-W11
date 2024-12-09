# Databricks notebook source
from pyspark.sql import functions as F

# Load the table
df = spark.table("peter_dev.tbl_house_prices")

# Drop rows with NA or null values
df_clean = df.dropna()

# Calculate mean and standard deviation of MedHouseVal grouped by HouseAge
stats_df = df_clean.groupBy("HouseAge").agg(
    F.mean("MedHouseVal").alias("mean_val"),
    F.stddev("MedHouseVal").alias("stddev_val")
)

# Join the stats back to the original dataframe
df_with_stats = df_clean.join(stats_df, on="HouseAge")

# Filter out rows where MedHouseVal is more than three standard deviations from the mean
df_filtered = df_with_stats.filter(
    (df_with_stats["MedHouseVal"] <= df_with_stats["mean_val"] + 3 * df_with_stats["stddev_val"]) &
    (df_with_stats["MedHouseVal"] >= df_with_stats["mean_val"] - 3 * df_with_stats["stddev_val"])
)

# Select relevant columns to exclude the stats columns
df_result = df_filtered.select(df_clean.columns)

# Create a new table
df_result.write.mode("overwrite").saveAsTable("peter_dev.tbl_house_prices_filtered")
