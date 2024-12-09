import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

# Databricks details
databricks_url = os.environ.get("SERVER_HOST")
token = os.environ.get("DATABRICKS_ACCESS_TOKEN")
job_id = os.environ.get("JOB_ID")

# Start job
start_job_url = f"https://{databricks_url}/api/2.1/jobs/run-now"
headers = {"Authorization": f"Bearer {token}"}
payload = {"job_id": job_id}

response = requests.post(start_job_url, headers=headers, json=payload, timeout=20)
if response.status_code == 200:
    RUN_ID = response.json().get("run_id")
    print(f"Job started successfully. Run ID: {RUN_ID}")
else:
    print(f"Error starting job: {response.text}")
    exit(1)

# Monitor job
def monitor_job(run_id):
    get_status_url = f"https://{databricks_url}/api/2.1/jobs/runs/get"
    while True:
        time.sleep(30)  # Poll every 30 seconds
        status_response = requests.get(
            get_status_url, headers=headers, params={"run_id": run_id}, timeout=20
        )
        if status_response.status_code == 200:
            state = status_response.json().get("state").get("life_cycle_state")
            print(f"Job status: {state}")
            if state == "TERMINATED":
                result_state = status_response.json().get("state").get("result_state")
                print(f"Job finished with result state: {result_state}")
                return result_state
            elif state in ["INTERNAL_ERROR", "SKIPPED", "FAILED"]:
                print(f"Job failed with state: {state}")
                return state
        else:
            print(f"Error fetching job status: {status_response.text}")
            return "ERROR"


# Check job status
job_status = monitor_job(RUN_ID)
if job_status == "SUCCESS":
    print("Job completed successfully!")
else:
    print("Job did not complete successfully.")
