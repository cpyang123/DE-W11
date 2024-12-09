# Databricks notebook source
import matplotlib.pyplot as plt
import pandas as pd

# Load the data into a Spark DataFrame
df = spark.table("peter_dev.tbl_house_prices_filtered")

# Convert to Pandas DataFrame for plotting
pdf = df.toPandas()

# Trend line chart: HouseAge vs mean of MedHouseVal
trend_data = pdf.groupby('HouseAge')['MedHouseVal'].mean().reset_index()
plt.figure(figsize=(10, 6))
plt.plot(trend_data['HouseAge'], trend_data['MedHouseVal'], marker='o')
plt.title('Trend Line: HouseAge vs Mean of MedHouseVal')
plt.xlabel('HouseAge')
plt.ylabel('Mean of MedHouseVal')
plt.grid(True)
plt.savefig('/Workspace/Users/chaopeter.yang@gmail.com/trend_line.png')
plt.close()

# Box and whisker chart: AveRooms (rounded) vs MedHouseVal
pdf['AveRoomsRounded'] = pdf['AveRooms'].round()
plt.figure(figsize=(12, 8))
pdf.boxplot(column='MedHouseVal', by='AveRoomsRounded', grid=False)
plt.title('Box and Whisker: AveRooms (rounded) vs MedHouseVal')
plt.suptitle('')
plt.xlabel('AveRooms (rounded)')
plt.ylabel('MedHouseVal')
plt.savefig('/Workspace/Users/chaopeter.yang@gmail.com/box_whisker.png')
plt.close()

# Save the report in a markdown file
with open('/Workspace/Users/chaopeter.yang@gmail.com/report.md', 'w') as f:
    f.write("# House Prices Report\n")
    f.write("## Trend Line: HouseAge vs Mean of MedHouseVal\n")
    f.write("![Trend Line](trend_line.png)\n")
    f.write("## Box and Whisker: AveRooms (rounded) vs MedHouseVal\n")
    f.write("![Box and Whisker](box_whisker.png)\n")
