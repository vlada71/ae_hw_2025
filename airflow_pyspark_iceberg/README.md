Overview
========

This project showcases running pyspark locally within a Docker container that also includes Airflow. The spark container includes Iceberg packages
which allow you to access the Rest catalog in Iceberg

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://docs.astronomer.io/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

1. Either 

```shell
git clone https://github.com/DataExpert-io/bootcamp-homework-v5.git
```
or if you've already cloned the project, run `git pull` to fetch and merge the new changes to your local repository

2. In the `.env` file, add your `SCHEMA` and save the file

3. Start Airflow on your local machine by running `astro dev start`.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

3a. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running `astro dev start` will start your project with the Airflow Webserver exposed at port 8081 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://docs.astronomer.io/astro/test-and-troubleshoot-locally#ports-are-not-available).

3b. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8081/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

3c. Go to Admin -> Connections, and add a new Spark connection. Click on `+` and add a new record
```shell
Connection Id spark-conn
Connection Type Spark
Host spark://spark-master:7077
```
Leave the reamining values as it is and hit `Save`

4. Go to the DAGs page, and unpause `run_pyspark_wap_in_airflow`. Click on the play button to manually trigger the dag

5. Run `astro dev stop` to stop the container


Contact
=======

If you run into any issues, head to the Discord channel
