import pytest
import requests
from unittest.mock import patch
import os
from dotenv import load_dotenv


load_dotenv()

DATABRICKS_URL = os.environ.get("SERVER_HOST")
TOKEN = os.environ.get("DATABRICKS_ACCESS_TOKEN")
JOB_ID = os.environ.get("JOB_ID")


# Headers for authentication
headers = {"Authorization": f"Bearer {TOKEN}"}

# Step 1: Get the most recent job run
# Fixture for headers
def get_latest_run_id(job_id):
    list_runs_url = f"https://{DATABRICKS_URL}/api/2.1/jobs/runs/list"
    params = {
        "job_id": job_id,
        "active_only": False,
        "limit": 1,
    }  # Limit to the latest run

    response = requests.get(list_runs_url, headers=headers, params=params)

    if response.status_code == 200:
        runs = response.json().get("runs", [])
        if runs:
            latest_run = runs[0]
            run_id = latest_run.get("run_id")
            print(f"Most recent run ID: {run_id}")
            return run_id
        else:
            print("No runs found for the specified job.")
            return None
    else:
        print(f"Error fetching job runs: {response.text}")
        return None


# Example usage
RUN_ID = get_latest_run_id(JOB_ID)
if RUN_ID:
    print(f"Latest Run ID: {RUN_ID}")
else:
    print("Could not retrieve the latest run ID.")


@pytest.fixture
def mock_headers():
    return {"Authorization": f"Bearer {TOKEN}"}


@pytest.fixture
def mock_start_job_response():
    return {"run_id": RUN_ID}


@pytest.fixture
def mock_get_status_response_success():
    return {"state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"}}


@pytest.fixture
def mock_get_status_response_failed():
    return {"state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"}}


@pytest.fixture
def mock_table_verification_response():
    return {
        "results": [
            ["peter_dev", "tbl_house_prices", "MANAGED"],
        ]
    }


@patch("requests.post")
def test_start_job_success(mock_post, mock_headers, mock_start_job_response):
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = mock_start_job_response

    response = requests.post(
        f"https://{DATABRICKS_URL}/api/2.1/jobs/run-now",
        headers=mock_headers,
        json={"job_id": JOB_ID},
    )

    assert response.status_code == 200
    assert response.json().get("run_id") == RUN_ID


@patch("requests.get")
def test_monitor_job_success(mock_get, mock_headers, mock_get_status_response_success):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_get_status_response_success

    response = requests.get(
        f"https://{DATABRICKS_URL}/api/2.1/jobs/runs/get",
        headers=mock_headers,
        params={"run_id": RUN_ID},
    )

    assert response.status_code == 200
    assert response.json()["state"]["life_cycle_state"] == "TERMINATED"
    assert response.json()["state"]["result_state"] == "SUCCESS"


@patch("requests.get")
def test_monitor_job_failure(mock_get, mock_headers, mock_get_status_response_failed):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_get_status_response_failed

    response = requests.get(
        f"https://{DATABRICKS_URL}/api/2.1/jobs/runs/get",
        headers=mock_headers,
        params={"run_id": RUN_ID},
    )

    assert response.status_code == 200
    assert response.json()["state"]["life_cycle_state"] == "TERMINATED"
    assert response.json()["state"]["result_state"] == "FAILED"


@patch("requests.post")
def test_verify_table_exists(mock_post, mock_headers, mock_table_verification_response):
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = mock_table_verification_response

    response = requests.post(
        f"https://{DATABRICKS_URL}/api/2.1/sql/statements",
        headers=mock_headers,
        json={"statement": "SHOW TABLES IN peter_dev"},
    )

    assert response.status_code == 200
    tables = response.json().get("results", [])
    assert any("tbl_house_prices" in table[1] for table in tables)
