from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from mylib.transform_load import transform_and_load
from databricks.display import display

def query_data():
    """Query data from Delta table."""
    spark = SparkSession.builder.appName("QueryData").getOrCreate()
    transform_and_load()
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
    plt.show()
    display(plt.gcf())
    #print("Visualization saved as 'nutrition_viz.png'.")

if __name__ == "__main__":
    query_results = query_data()
    visualize(query_results)