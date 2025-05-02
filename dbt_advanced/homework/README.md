**Airflow + dbt**
====================

This repository emulates an “open-source” project, though exclusively shared within the dataexpert community. Members can access the repository for independent use or contribute enhancements to the project's design and functionality. This serves as an opportunity to practice contributing to publicly shared open-source repositories.

**Table of Contents**

- [🚀 Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Local Development](#local-development)
  - [Debugging](#debugging)
- [dbt Project Setup](#dbt-project-setup)
- [Homework Submission](#homework-submission)
  - [Steps](#steps)
    - [Create the audit table](#create-the-audit-table)
    - [Create the production table](#create-the-production-table)
    - [Create the WAP pipeline with airflow](#create-the-wap-pipeline-with-airflow)
  - [Submission](#submission)
- [📚 Other helpful resources for learning!](#-other-helpful-resources-for-learning)
  - [dbt docs](#dbt-docs)
  - [📂 Navigating the Repository](#-navigating-the-repository)



# **🚀 Getting Started**

## **Prerequisites**

1. **Install [Docker](https://docs.docker.com/engine/install/)**: Docker is a platform for packaging, distributing, and managing applications in containers.
2. **Install the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)**: Astro CLI is a command-line tool designed for working with Apache Airflow projects, streamlining project creation, deployment, and management for smoother development and deployment workflows.

## **Local Development**

1. **Clone the Repository**: Open a terminal, navigate to your desired directory, and clone the repository using:
    ```bash
    git clone git@github.com:DataExpert-io/airflow-dbt-project.git # clone the repo
    cd airflow-dbt-project # navigate into the new folder
    ```

    1. If you don’t have SSH configured with the GitHub CLI, please follow the instructions for [generating a new SSH key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent) and [adding a new SSH key to your GitHub account](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account?tool=cli) in the GitHub docs.


  2. **Create a Branch:**
    - Navigate to the **`bootcamp-homework-v5/dbt_advanced/homework`** folder on your local machine.
    - Use the **`git checkout -b`** command to create a new branch where you can commit and push your changes. Prefix your branch name with your Git username to avoid conflicts.
    For example:
        ```bash
        git checkout -b homework/my-git-username
        ```
     Create a copy of the **`template/`** folder and rename it to **`<your-git-username>`**, for example **`dbt_advanced/homework/john`**.

3. **Docker Setup and Management**: Launch Docker Daemon or open the Docker Desktop app
4. **Run the Astro Project**:
    - Navigate to the **`bootcamp-homework-v5/dbt_advanced/homework/<your_username>`** folder on your local machine.
    - Start Airflow on your local machine by running **`astro dev start`**
        - This will spin up 4 Docker containers on your machine, each for a different Airflow component:
            - **Postgres**: Airflow's Metadata Database, storing internal state and configurations.
            - **Webserver**: Renders the Airflow UI.
            - **Scheduler**: Monitors, triggers, and orchestrates task execution for proper sequencing and resource allocation.
            - **Triggerer**: Triggers deferred tasks.
        - Verify container creation with **`docker ps`**
    - **Access the Airflow UI**: Go to http://localhost:8081/ and log in with '**`admin`**' for both Username and Password
        >
        > ℹ️ Note: Running astro dev start exposes the Airflow Webserver at port **`8081`** and Postgres at port **`5431`**.
        >
        > If these ports are in use, halt existing Docker containers or modify port configurations in **`.astro/config.yaml`**.
        >
4. **Stop** the Astro Docker container by running `**astro dev stop**`
    >
    > ❗🚫❗  Remember to stop the Astro project after working to prevent issues with Astro and Docker ❗🚫❗
    >


**⭐️ TL;DR - Astro CLI Cheatsheet ⭐️**

```bash
astro dev start # Start airflow
astro dev stop # Stop airflow
astro dev restart # Restart the running Docker container
astro dev kill # Remove all astro docker components
```

### **Debugging**

If the Airflow UI isn't updating, the project seems slow, Docker behaves unexpectedly, or other issues arise, first remove Astro containers and rebuild the project:

- Run these commands:
    ```bash
    # Stop all locally running Airflow containers
    astro dev stop

    # Kill all locally running Airflow containers
    astro dev kill

    # Remove Docker container, image, and volumes
    docker ps -a | grep dataexpert-airflow-dbt | awk '{print $1}' | xargs -I {} docker rm {}
    docker images | grep ^dataexpert-airflow-dbt | awk '{print $1}' | xargs -I {} docker rmi {}
    docker volume ls | grep dataexpert-airflow-dbt | awk '{print $2}' | xargs -I {} docker volume rm {}

    # In extreme cases, clear everything in Docker
    docker system prune
    ```

- Restart Docker Desktop.
- (Re)build the container image without cache.
    ```bash
    astro dev start --no-cache
    ```


## dbt Project Setup

- Go to the project's directory
  ```bash
  cd dbt_project
  ```
- Create a venv to isolate required packages
  ```bash
  python3 -m venv venv # MacOS/Linux
  # or
  python -m venv venv # Windows/PC
  ```
- Source the virtual environment
  ```bash
  source venv/bin/activate # MacOS/Linux
  # or
  venv/Scripts/activate # Windows/PC
  ```
- Install the required packages
  ```bash
  pip3 install -r dbt-requirements.txt # MacOS/Linux
  # or
  pip install -r dbt-requirements.txt # Windows/PC
  ```
- Update `DBT_SCHEMA` environment variable
  - MacOS/Linux:
    - Open the `dbt.env` file, change the `DBT_SCHEMA` to your schema from Weeks 1 and 2, and source the environment variables to your local (terminal) environment
      ```bash
      export DBT_SCHEMA='your_schema' # EDIT THIS FIELD
      ```
    - then run
      ```bash
      source dbt.env
      ```
  - Windows/PC:
    - Instead of overwriting the DBT_SCHEMA in the file you can run:
      - CMD:
      ```bash
      set DBT_SCHEMA=your_schema

      # For example
      set DBT_SCHEMA=john #(without quotes)
      ```
      - PowerShell:
      ```bash
      $env:DBT_SCHEMA = "your_schema"
      ```


- Run `dbt debug` to check your connection. You should see a message like this:
    ```
    13:43:43  Running with dbt=1.9.0-b3
    13:43:43  dbt version: 1.9.0-b3
    13:43:43  python version: 3.9.6
    13:43:43  python path: .../dbt_advanced_/venv/bin/python3
    13:43:43  os info: macOS-15.1-arm64-arm-64bit
    13:43:44  Using profiles dir at .
    13:43:44  Using profiles.yml file at ./profiles.yml
    13:43:44  Using dbt_project.yml file at ./dbt_project.yml
    13:43:44  adapter type: snowflake
    13:43:44  adapter version: 1.8.4
    13:43:44  Configuration:
    13:43:44    profiles.yml file [OK found and valid]
    13:43:44    dbt_project.yml file [OK found and valid]
    13:43:44  Required dependencies:
    13:43:44   - git [OK found]

    13:43:44  Connection:
    13:43:44    account: aab46027.us-west-2
    13:43:44    user: dataexpert_student
    13:43:44    database: DATAEXPERT_STUDENT
    13:43:44    warehouse: COMPUTE_WH
    13:43:44    role: ALL_USERS_ROLE
    13:43:44    schema: john
    13:43:44    authenticator: None
    13:43:44    oauth_client_id: None
    13:43:44    query_tag: john
    13:43:44    client_session_keep_alive: False
    13:43:44    host: None
    13:43:44    port: None
    13:43:44    proxy_host: None
    13:43:44    proxy_port: None
    13:43:44    protocol: None
    13:43:44    connect_retries: 0
    13:43:44    connect_timeout: 10
    13:43:44    retry_on_database_errors: False
    13:43:44    retry_all: False
    13:43:44    insecure_mode: False
    13:43:44    reuse_connections: True
    13:43:44  Registered adapter: snowflake=1.8.4
    13:43:50    Connection test: [OK connection ok]

    13:43:50  All checks passed!
    ```


You're good to go!

# **Homework Submission**

For this week's assignment we will use the same project from the dbt basics homework, the one about haunted houses. You will create a WAP pipeline update the Visits's fact orders table.

Remember that, if you are running your project for the first time, dbt will ask you to run

```bash
$ dbt deps
```

to install the packages defined in packages.yml

## Steps

### Create the audit table

Let's change the current `fact_visits.sql` model.
- Rename the `fact_visits.sql` to `audit_fact_visits.sql`
- Rename the `fact_visits.yml` to `audit_fact_visits.yml`
    - Inside `audit_fact_visits.yml`, change `- name: fact_visits` to `- name: audit_fact_visits`

This model should select from the staging models you have in your project, but only for a specific `purchase_date`. You can use `'2024-11-06'` for this assignment. In a real application, we would probably set it as `current_date`.

You will notice `stg_customer_feedbacks` does not have a date column. So you should select the rows where the `ticket_id` matches the `ticket_id` of your selection CTE from `stg_haunted_house_tickets`.

The YML file already contains tests, like I assume you have done for the basics homework.

### Create the production table

Create a new `fact_visits` model to be our production table. This model should select everything from the audit model.

This model should be an incremental model (see more about incremental models here https://docs.getdbt.com/docs/build/materializations#incremental), you can use any incremental strategy you want.

### Create the WAP pipeline with airflow

You will create 2 airflow dags

1. One DAG called `dbt_project_dag.py` that should run the whole project.
2. One DAG called `dbt_wap_dag.py` that should build (run and test) fact_visits and all upstream models (stg_haunted_house_tickets and stg_customer_feedbacks).

To create the dags, create the files `dbt_project_dag.py` and `dbt_wap_dag.py` file inside the `dags/dbt/` folder. You can use any dbt dag from the repo https://github.com/DataExpert-io/airflow-dbt-project.git as a template for them. You can use the BashOperator or Cosmos.

Note that you need to change the profile_name to match the one in the `profiles.yml` file, which is 'my-snowflake-db'.

### Submission

To submit your work, **compress ONLY THE FOLLOWING FILES** into a ZIP file. Upload the ZIP file in the assignments page.
- `audit_fact_visits.sql`
- `audit_fact_visits.yml`
- `fact_visits.sql`
- `dbt_project_dag.py`
- `dbt_wap_dag.py`

Again, compress only these files, not the entire project. This will make the auto-grader faster.


<br>
<hr />



# 📚 Other helpful resources for learning!

### dbt docs
- [dbt best practices for enterprises](https://www.phdata.io/blog/accelerating-and-scaling-dbt-for-the-enterprise/)
- [dbt cheat sheet](https://github.com/bruno-szdl/cheatsheets/blob/main/dbt_cheat_sheet.pdf)
- [models](https://docs.getdbt.com/docs/build/sql-models)
- [tests](https://docs.getdbt.com/docs/build/data-tests)
- [sources](https://docs.getdbt.com/docs/build/sources)
- [seeds](https://docs.getdbt.com/docs/build/seeds)
- [snapshots](https://docs.getdbt.com/docs/build/snapshots)
- [dbt_project.yml](https://docs.getdbt.com/reference/dbt_project.yml)
- [profiles,yml](https://docs.getdbt.com/docs/core/connect-data-platform/profiles.yml)
- [Commands](https://docs.getdbt.com/reference/commands/build)
- [Node selection](https://docs.getdbt.com/reference/node-selection/syntax)

### 📂 Navigating the Repository

Each dbt project contains various directories and files. Learn more about the structure of the project below:

- **`dbt_packages/`**: This folder is where dbt install packages (outside projects).
- **`logs/`**: This folder is where dbt store logs.
- **`macros/`**: This folder is where dbt searches for custom macros.
- **`models/`**: This folder is where dbt searches for models. You can create subfolders in the way you want, no problem.
- **`seeds/`**: This folder is where dbt searches for seeds.
- **`snapshots/`**: This folder is where dbt searches for snapshots.
- **`target/`**: This folder is where dbt stores [artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts) and the compiled SQL code (the code dbt sends to the data warehouse to run).
- **`tests/`**: This folder is where dbt searches for custom tests (generic or singular).
- **`dbt_project.yml`**: Every dbt project needs a dbt_project.yml file — this is how dbt knows a directory is a dbt project. It also contains important information that tells dbt how to operate your project. [More info here](https://docs.getdbt.com/reference/dbt_project.yml).
- **`packages.yml`**: This folder is where you define the packages you want dbt to install.
