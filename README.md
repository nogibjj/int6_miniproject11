# int6_miniproject11
[![CI](https://github.com/nogibjj/int6_miniproject10/actions/workflows/ci.yml/badge.svg)](https://github.com/nogibjj/int6_miniproject10/actions/workflows/ci.yml)

### File Structure
```
int6_miniproject11
├── .devcontainer/
│   ├── devcontainer.json
│   └── Dockerfile
├── .github/
│   └── workflows/ci.yml
├── .env
├── .gitignore
├── LICENSE
├── Makefile
├── README.md
├── data
│   └── Nutrition.csv
├── main.py
├── mylib
│   ├── __init__.py
│   ├── __pycache__
│   ├── extract.py
│   ├── transform_load.py
│   └── query.py
├── run_job.py
├── requirements.txt
└── test_main.py
```
This repo contains work for mini-project 11. It sets up an environment on codespaces and uses Github Actions to run a Makefile for the following: `make install`, `make format`, `make lint`, `make test`, and `make job` . The dataset was sourced from the [fivethirtyeight github](https://github.com/fivethirtyeight/data/blob/master/nutrition-studies/raw_anonymized_data.csv) and details individuals' health and nutrition information.

## Purpose of project
The purpose of this project is to create a data pipeline with Databricks to demonstrate extraction from a data source, transformation/loading into a data sink using PySpark, and querying using SQL.

## **Overview**
- **Data**
  - The dataset is sourced from the [FiveThirtyEight GitHub repository](https://github.com/fivethirtyeight/data/blob/master/nutrition-studies/raw_anonymized_data.csv) and includes information about individuals' health and nutrition habits.
- **Data Extraction**
  - `extract()`: Using REST APIs to extract data from github URL and upload data to Databricks FileStore (DBFS).
- **Data Transformation**
  - `transform_and_load()`: Processing data in PySpark and saving it as a Delta table for querying. Sample transformation includes calculations to summarize healthy and unhealthy food consumption and incorporates health condition flags.
- **Data Querying**
  - `query_data()`: Using SQL queries on Delta tables to show how dietary patterns differ based on health conditions.
- **Visualization**
  - `visualize()`: Generating visualizations of query results using Matplotlib.
- **Testing**
  - These functions are tested in `test_main.py`. 
- **CI/CD Automation**
  - To make sure github actions is working properly, I use a Makefile to test various parts of my code. `run_job.py` programmatically triggers a Databricks jobs.

## Preparation
1. Create a Databricks workspace and connect to Github
2. Set up env variables
3. Create a Databricks cluster that supports Pyspark
4. Clone repo into Databricks workspace
5. Create a job on Databricks to build pipeline with the following tasks:
    - Extract task (Data Source): mylib/extract.py
    - Transform and Load Task (Data Sink): mylib/transform_load.py
    - Query Task: mylib/query.py

## Outputs
### Query Results

| Has Health Condition | Avg Healthy | Avg Unhealthy |
|---------------------|-------------|---------------|
| true                | 17.77       | 10.18         |
| false               | 20.93       | 10.73         |

### Query Visualization
<img width="600" alt="query visualization image" src=query_barplot.png>

## Sample Job Run
<img width="600" alt="successful run" src=job_run.png>
<img width="600" alt="successful run" src=job_run_timeline.png>

## Check format and test errors 
1. Format code `make format`
2. Lint code `make lint`
3. Test code `make test`

<img width="600" alt="passing test cases image" src=pass_test.png>