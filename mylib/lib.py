import os
import requests
import io
import pandas as pd
import base64
from dotenv import load_dotenv
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


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
    # Open and read the local file as bytes
    with open(local_file, "rb") as f:
        content = f.read()

    # Create the file in DBFS
    handle_response = requests.post(
        f"https://{SERVER_HOSTNAME}/api/2.0/dbfs/create",
        headers=headers,
        json={"path": dbfs_path, "overwrite": True}
    )
    if handle_response.status_code != 200:
        print(f"Error creating file in DBFS: {handle_response.text}")
        return
    
    handle = handle_response.json()["handle"]

    # Add the file content in base64-encoded blocks
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

    # Close the file handle
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
    # Check if file already exists in DBFS
    if check_filestore_path(DBFS_FILE_PATH, headers):
        print(f"File already exists at {DBFS_FILE_PATH}. Skipping upload.")
        return

    # Download CSV file
    response = requests.get(CSV_URL)
    response.raise_for_status()

    # Save locally
    with open(LOCAL_FILE_PATH, "wb") as f:
        f.write(response.content)
    
    # Upload to DBFS
    upload_to_dbfs(LOCAL_FILE_PATH, DBFS_FILE_PATH)


def transform_and_load():
    spark = SparkSession.builder.appName("TransformLoad").getOrCreate()
    """Transform data and save it as a Delta table."""
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(DBFS_FILE_PATH)

    # Transform data
    df = df.withColumn("healthy_total", col("EGGSFREQ") + col("GREENSALADFREQ") + col("MILKFREQ") + col("COFFEEFREQ")) \
           .withColumn("unhealthy_total", col("FRIESFREQ") + col("SODAFREQ") + col("CAKESFREQ")) \
           .withColumn("has_health_condition", 
                       (col("cancer") == "Yes") | (col("diabetes") == "Yes") | (col("heart_disease") == "Yes"))

    # Write to Delta table
    spark.sql("DROP TABLE IF EXISTS nutrition_delta")
    df.write.format("delta").mode("overwrite").saveAsTable("nutrition_delta")
    print("Data transformed and loaded into Delta table 'nutrition_delta'.")


def query_data():
    spark = SparkSession.builder.appName("QueryData").getOrCreate()
    """Query data from Delta table."""
    query = """
        SELECT has_health_condition, AVG(healthy_total) AS avg_healthy, AVG(unhealthy_total) AS avg_unhealthy
        FROM nutrition_delta
        GROUP BY has_health_condition
    """
    df = spark.sql(query)
    print("Query Results:")
    df.show()
    return df.toPandas()


def visualize(df):
    """Visualize query results."""
    df.plot(x="has_health_condition", y=["avg_healthy", "avg_unhealthy"], kind="bar", figsize=(10, 6))
    plt.title("Average Healthy vs. Unhealthy Food Totals")
    plt.xlabel("Has Health Condition")
    plt.ylabel("Average Food Totals")
    plt.savefig("nutrition_viz.png")
    print("Visualization saved as 'nutrition_viz.png'.")


if __name__ == "__main__":
    extract()
    transform_and_load()
    query_results = query_data()
    visualize(query_results)
