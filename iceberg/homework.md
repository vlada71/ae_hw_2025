# Homework Week 1

Startup:

- To start the homework you'll need to do:
`pip install -r requirements.txt`


# Requirements 
Using PyIceberg (or PySpark), the Polygon API and Tabular

Do the following things:

- Create a daily partitioned table in your username's schema for MAANG stock prices from Polygon
- Make a new audit branch for that table with PyIceberg `create_branch` function (or using PySpark functions)
- In the script that adds a new daily summary file to the daily partition every day
- Save this file as `stock_prices.py` and zip it up and upload to https://www.dataexpert.io/assignments 

For **bonus points**:
- In a separate script, write a PySpark job that
  - Fast forwards the audit branch to main

