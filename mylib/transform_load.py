from pyspark.sql.functions import col
from pyspark.sql import SparkSession

DBFS_FILE_PATH = "dbfs:/FileStore/tables/nutrition.csv"

def transform_and_load():
    """Transform data and save it as a Delta table."""
    spark = SparkSession.builder.appName("TransformLoad").getOrCreate()
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(DBFS_FILE_PATH)

    df = df.withColumn("healthy_total", col("EGGSFREQ") + col("GREENSALADFREQ") + col("MILKFREQ") + col("COFFEEFREQ")) \
           .withColumn("unhealthy_total", col("FRIESFREQ") + col("SODAFREQ") + col("CAKESFREQ")) \
           .withColumn("has_health_condition", 
                       (col("cancer") == "Yes") | (col("diabetes") == "Yes") | (col("heart_disease") == "Yes"))

    spark.sql("DROP TABLE IF EXISTS nutrition_delta")
    df.write.format("delta").mode("overwrite").saveAsTable("nutrition_delta")
    print("Data transformed and loaded into Delta table 'nutrition_delta'.")

if __name__ == "__main__":
    transform_and_load()