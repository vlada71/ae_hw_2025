# Homework Week 1

Startup:

- To start working on this homework you'll need to:
- Copy the `starter_dag.py` file into `airflow-dbt-project/dags` and use `astro dev start` to start working

# Requirements 
Using Airflow, the Polygon API and Tabular

Do the following things:
- Create a daily partitioned table in Iceberg that tracks the price of MAANG stocks (and others if you want)
- Create a script that loads the data from polygon into the Iceberg staging table
- Run data quality checks on the data making sure everything looks legit
- Exchange the data from the staging into production
- Write a cumulation script to pull the price data into arrays (storing the rolling last 7 elements) incrementally for faster computation 

For **bonus points**:
- Use PySpark instead of PyIceberg for loading the data into Iceberg
- Use WAP pattern with PySpark and the Iceberg branch model instead of exchanges with Trino

To submit:
- include the `starter_dag.py` file (and the Spark files if you're doing the bonus) in a zip file
- submit at the [assignment page](https://www.dataexpert.io/assignments)