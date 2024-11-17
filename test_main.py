import os
import pytest
from mylib.lib import (
    extract,
    transform_and_load,
    query_data,
    visualize,
)


@pytest.fixture(scope="module")
def spark():
    """Fixture to provide the pre-initialized Spark session in Databricks."""
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def test_extract():
    """Test the extract function to ensure data is uploaded to DBFS."""
    # Run the extraction step
    extract()

    # Assert the file exists in DBFS
    dbfs_file_path = "dbfs:/FileStore/tables/nutrition.csv"
    assert os.path.exists(f"/dbfs{dbfs_file_path[5:]}") is True


def test_transform_and_load(spark):
    """Test the transform and load step."""
    # Run transformation and loading to Delta table
    transform_and_load()

    # Assert that the Delta table exists
    result = spark.sql("SHOW TABLES LIKE 'nutrition_delta'")
    assert result.count() > 0


def test_query_data(spark):
    """Test querying the Delta table."""
    # Run the query
    result = query_data()

    # Assert that the query returned results
    assert result is not None
    assert len(result) > 0


def test_visualize(spark):
    """Test the visualization step."""
    # Run a query to get data
    result = query_data()

    # Generate visualization
    visualize(result)

    # Assert the visualization file was created
    assert os.path.exists("nutrition_viz.png") is True
