# int6_miniproject11
[![CI](https://github.com/nogibjj/int6_miniproject10/actions/workflows/ci.yml/badge.svg)](https://github.com/nogibjj/int6_miniproject10/actions/workflows/ci.yml)

### File Structure
```
int6_miniproject10
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
│   └── lib.py
├── run_job.py
├── requirements.txt
└── test_main.py
```
This repo contains work for mini-project 11. It sets up an environment on codespaces and uses Github Actions to run a Makefile for the following: `make install`, `make format`, `make lint`, `make test`, and `make job` . The dataset was sourced from the [fivethirtyeight github](https://github.com/fivethirtyeight/data/blob/master/nutrition-studies/raw_anonymized_data.csv) and details individuals' health and nutrition information.

Some important components:

* `Makefile`

* `Dockerfile`

* A base set of libraries for devops and web

* `githubactions` 

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
1. Create a Databricks workspace on Azure
2. Connect Github account to Databricks Workspace
3. Create global init script for cluster start to store enviornment variables
4. Create a Databricks cluster that supports Pyspark
5. Clone repo into Databricks workspace
6. Create a job on Databricks to build pipeline
7. Extract task (Data Source): mylib/extract.py
8. Transform and Load Task (Data Sink): mylib/transform_load.py
9. Query and Viz Task: mylib/query_viz.py

## Check format and test errors 
1. Format code `make format`
2. Lint code `make lint`
3. Test code `make test`

<img width="600" alt="passing test cases image" src=pass_test.png>

## Sample Job Run


## Outputs
Query visualization:
<img width="600" alt="query visualization image" src=query_viz.png>