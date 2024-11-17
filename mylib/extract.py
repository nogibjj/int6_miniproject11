import os
import requests
import base64
from dotenv import load_dotenv

# Load environment variables for REST API
load_dotenv()
SERVER_HOSTNAME = os.getenv("SERVER_HOSTNAME")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Headers for REST API calls
headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}

# Constants
CSV_URL = "https://raw.githubusercontent.com/fivethirtyeight/data/master/nutrition-studies/raw_anonymized_data.csv"
LOCAL_FILE_PATH = "/tmp/nutrition.csv"
DBFS_FILE_PATH = "dbfs:/FileStore/tables/nutrition.csv"

def check_filestore_path(path, headers, host=SERVER_HOSTNAME):
    """Check if a given path exists in Databricks FileStore."""
    try:
        response = requests.get(
            f"https://{host}/api/2.0/dbfs/get-status?path={path}",
            headers=headers
        )
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"File path does not exist or is inaccessible: {e}")
        return False

def upload_to_dbfs(local_file, dbfs_path):
    """Uploads a local file to Databricks FileStore using REST API."""
    with open(local_file, "rb") as f:
        content = f.read()

    handle_response = requests.post(
        f"https://{SERVER_HOSTNAME}/api/2.0/dbfs/create",
        headers=headers,
        json={"path": dbfs_path, "overwrite": True}
    )
    if handle_response.status_code != 200:
        print(f"Error creating file in DBFS: {handle_response.text}")
        return
    
    handle = handle_response.json()["handle"]

    block_response = requests.post(
        f"https://{SERVER_HOSTNAME}/api/2.0/dbfs/add-block",
        headers=headers,
        json={
            "handle": handle,
            "data": base64.b64encode(content).decode("utf-8")
        }
    )
    if block_response.status_code != 200:
        print(f"Error adding file block in DBFS: {block_response.text}")
        return

    close_response = requests.post(
        f"https://{SERVER_HOSTNAME}/api/2.0/dbfs/close",
        headers=headers,
        json={"handle": handle}
    )
    if close_response.status_code != 200:
        print(f"Error closing file in DBFS: {close_response.text}")
    else:
        print(f"File successfully uploaded to {dbfs_path}.")

def extract():
    """Download CSV and upload it to DBFS."""
    if check_filestore_path(DBFS_FILE_PATH, headers):
        print(f"File already exists at {DBFS_FILE_PATH}. Skipping upload.")
        return

    response = requests.get(CSV_URL)
    response.raise_for_status()

    with open(LOCAL_FILE_PATH, "wb") as f:
        f.write(response.content)
    
    upload_to_dbfs(LOCAL_FILE_PATH, DBFS_FILE_PATH)

if __name__ == "__main__":
    extract()