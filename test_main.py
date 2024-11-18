import requests
from dotenv import load_dotenv
import os
from mylib.extract import extract

load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/tables/nutrition.csv"
url = f"https://{server_h}/api/2.0"

def test_databricks_workspace():
    extract()

    try:
        requests.get(
            url + "/workspace/get-status",
            params={"path": "/Workspace/Users/int6@duke.edu/int6_miniproject11"},
        )

    except Exception as e:
        assert False, f"Failed to verify workspace path: {e}"

def test_databricks_file():
    headers = {'Authorization': f'Bearer {access_token}'}
    try:
        requests.get(
            url + f"/dbfs/get-status?path={FILESTORE_PATH}",
            headers=headers
        )

    except Exception as e:
        assert False, f"Failed to verify data path: {e}"

if __name__ == "__main__":
    test_databricks_workspace()
    test_databricks_file()