[![Python Application Test with Github Actions for DE-W11](https://github.com/cpyang123/DE-W11/actions/workflows/test.yml/badge.svg)](https://github.com/cpyang123/DE-W11/actions/workflows/test.yml)

#  Databricks Job Execution

Link to the default job on Databricks (Simple ETL): https://dbc-c95fb6bf-a65d.cloud.databricks.com/jobs/914595435648149?o=3670519680858392

## Overview
The `main.py` script orchestrates the execution of Databricks jobs for data processing, cleaning, visualization, and reporting. Each job is executed sequentially, and the results are stored in the Databricks environment.

Additionally, `test_main.py` is provided for testing the functionality of `main.py`, including API interactions and table creation.

---

## How to Use `main.py`

### Prerequisites
1. **Databricks Environment**: Ensure you have access to a Databricks workspace.
2. **Personal Access Token**: Generate a personal access token from Databricks.
3. **Job Configuration**: Update local `.env` script with the correct Databricks URL and Job IDs.
By default, you should have these in your .env file:

```{bash}
DATABRICKS_ACCESS_TOKEN = <Personal Access Token>
SERVER_HOST = dbc-c95fb6bf-a65d.cloud.databricks.com
HTTP_PATH = /sql/1.0/warehouses/2d6f41451e6394c0
JOB_ID = 914595435648149
```

### Steps to Run
1. **Run the Script**:
   Execute `main.py` from your terminal or Python environment:
   ```bash
   make run
   ```
2. **Monitor Jobs**:
   The script will start each job and monitor its progress until completion.
3. **Check Results**:
   After successful execution, the outputs (Delta tables, visualizations, and reports) will be available in your Databricks workspace.

---

## What the Jobs Do

### 1. **Data Extraction (`extract_databricks.py`)**
   - **Purpose**: Loads raw CSV data into a Delta table.
   - **Process**:
     1. Reads a subset of the `train.csv` file into a Pandas DataFrame.
     2. Converts the Pandas DataFrame into a Spark DataFrame.
     3. Saves the Spark DataFrame as a Delta table named `peter_dev.tbl_house_prices`.
   - **Output**: Delta table `tbl_house_prices` in the `peter_dev` database.

### 2. **Data Transformation and Cleaning (`Transform_and_Clean.py`)**
   - **Purpose**: Cleans and filters the data for outliers and null values.
   - **Process**:
     1. Removes rows with null values.
     2. Calculates statistical properties (mean, standard deviation) of house prices grouped by age.
     3. Filters out rows where house prices are beyond three standard deviations from the mean.
     4. Saves the filtered dataset as a new Delta table `peter_dev.tbl_house_prices_filtered`.
   - **Output**: Cleaned Delta table `tbl_house_prices_filtered` in the `peter_dev` database.

### 3. **Visualization and Reporting (`Load_Viz.py`)**
   - **Purpose**: Generates visual insights and a summary report.
   - **Process**:
     1. Reads the cleaned data from `tbl_house_prices_filtered`.
     2. Creates visualizations:
        - **Trend Line**: House Age vs. Median House Value.
        - **Box and Whisker Chart**: Average Rooms (rounded) vs. Median House Value.
     3. Saves the visualizations and a markdown report in the Databricks workspace.
   - **Output**:
     - Visualizations as PNG files (`trend_line.png`, `box_whisker.png`).
     - A markdown report summarizing the findings.

---

## How to run tests

### Overview
The `test_main.py` script provides unit tests for `main.py`, ensuring that it interacts correctly with the Databricks REST API, monitors job statuses, and verifies table creation. The tests use mock data to simulate API responses, making them safe to run without affecting the Databricks workspace.

### Prerequisites
1. **Install Dependencies**:
   - Ensure `pytest` and all other dependencies are installed:
     ```bash
     make install
     ```

2. **Environment Configuration**:
   Ensure that `.env` is correctly configured before running the tests.

### Running the Tests
1. Run the tests using `pytest`:
   ```bash
   pytest test_main.py
   # or 
   make test
   ```

2. **What the Tests Cover**:
   - **Job Submission**: Verifies that a job can be started successfully using the Databricks API.
   - **Job Monitoring**: Ensures that job status is monitored and errors are handled gracefully.
   - **Table Verification**: Confirms that the expected Delta table is created and accessible.

3. **Mocked Behavior**:
   - The tests use the `unittest.mock` library to simulate Databricks API responses.
   - This ensures the tests do not require actual API calls, making them faster and safer to run.

---

## Key Notes
- **Error Handling**: Both `main.py` and `test_main.py` include robust error handling to deal with API failures and unexpected behavior.
- **Code Modularity**: The scripts are designed for modularity, making them easy to adapt to other use cases.
- **Dependencies**:
  - Ensure `pandas`, `matplotlib`, `pytest`, and Spark libraries are installed in your environment.

---

For additional help or customization, feel free to contact your Databricks administrator.

