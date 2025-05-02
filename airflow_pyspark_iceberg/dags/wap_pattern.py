"""This example demos the WAP pattern assuming the DQ checks have passed. The goal
is to demonstrate running Pyspark locally in Docker along with Airflow and Iceberg"""
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from datetime import datetime
from include.eczachly.trino_queries import execute_trino_query
from dotenv import load_dotenv
import os
load_dotenv()

catalog_name = Variable.get("CATALOG_NAME")
tabular_credential = Variable.get("TABULAR_CREDENTIAL")
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")

schema = os.getenv('SCHEMA')
if not schema:
    raise ValueError('Set your schema in the .env file')
table = "pyspark_wap"
branch = 'audit'

packages_lst = [
    "org.apache.iceberg:iceberg-aws-bundle:1.7.1",
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1",
    # "org.apache.hadoop:hadoop-aws:3.3.4",
    # "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]

SPARK_HOME = '/opt/bitnami/spark'
jars_lst = [
    f"{SPARK_HOME}/jars/custom_jars/iceberg-aws-bundle-1.7.1.jar",
    f"{SPARK_HOME}/jars/custom_jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar"

]

packages = ",".join(packages_lst)
jars = ",".join(jars_lst)

spark_configurations = [
    ('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'),
    ('spark.sql.defaultCatalog', catalog_name),
    ('spark.sql.catalog.' + catalog_name, 'org.apache.iceberg.spark.SparkCatalog'),
    ('spark.sql.catalog.' + catalog_name + '.credential', tabular_credential),
    ('spark.sql.catalog.' + catalog_name + '.catalog-impl', 'org.apache.iceberg.rest.RESTCatalog'),
    ('spark.sql.catalog.' + catalog_name + '.warehouse', catalog_name),
    ('spark.sql.catalog.' + catalog_name + '.uri', 'https://api.tabular.io/ws/'),
    ('spark.sql.catalog.' + catalog_name + '.local.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO'),
    ('spark.sql.shuffle.partitions', '50'),
    ('spark.jars.packages', packages),
    ('spark.wap.branch', branch)
]

spark_conf_dict = dict(spark_configurations)
@dag(
    start_date=datetime(2025, 1, 21),
    schedule=None,
    catchup=False
)
def run_pyspark_wap_in_airflow():

    current_ds = '{{ ds }}'
    @task.pyspark(conn_id="spark-conn", config_kwargs=spark_conf_dict)
    def run_sql_stmt(spark: SparkSession, 
                     sc, 
                     catalog_name="academy", 
                     schema=schema, 
                     table=table,
                     **kwargs
                    ):

        query = f"""
                    select count(*) from {schema}.{table}
                """
        query = f"""create table if not exists {schema}.{table}(
                    id int,
                    value int,
                    date date
            )
            using iceberg
            partitioned by (date)
            TBLPROPERTIES ('write.wap.enabled'='true')
            """
        print(query)
        result = spark.sql(query)

        return result.show()
    
    @task.pyspark(conn_id="spark-conn", config_kwargs=spark_conf_dict)
    def delete_table(
        spark:SparkSession, 
        sc:SparkContext, 
        schema=schema, 
        table=table, 
        branch='audit',
        current_ds=None
        ):
        is_branch = spark.conf.get('spark.wap.branch')
        if is_branch:
            spark.conf.unset('spark.wap.branch')
            print('Switch to main branch')
        query = f"""
                    delete from {schema}.{table}
                    where date = '{current_ds}'
            """
        print(query)
        spark.sql(query)
        spark.conf.set('spark.wap.branch', branch)
        print('Switch to audit branch')
    
    @task.pyspark(conn_id="spark-conn", config_kwargs=spark_conf_dict)
    def create_wap_branch(
        spark:SparkSession, 
        sc:SparkContext, 
        schema=schema, 
        table=table, 
        branch=branch
        ):
        # alter_tbl_properties = f"""
        #             alter table {schema}.{table} set TBLPROPERTIES ('write.wap.enabled'='true')
        #     """
        # spark.sql(alter_tbl_properties)
        query = f"""
                    alter table {schema}.{table} create or replace branch {branch}
            """
        spark.sql(query)
    
    
    @task.pyspark(conn_id="spark-conn", config_kwargs=spark_conf_dict)
    def write_to_branch(spark:SparkSession, sc:SparkContext, schema=schema, table=table, current_ds=None) -> None:
        df = spark.createDataFrame(
            [
                (1, 100, current_ds),
                (2, 200, current_ds),
                (3, 300, current_ds),
            ],
            ["id", "value", "date"],
        )
        (
            df
            .withColumn('date', to_date('date'))
            .writeTo(f'{schema}.{table}')
            .option('branch', f"{branch}")
            .using('iceberg')
            .partitionedBy("date")
            .overwritePartitions()
        )
        branch_df = spark.read \
                .option('branch', f"{branch}") \
                .format('iceberg') \
                .table(f'{schema}.{table}')
        print('Output from audit branch')
        branch_df.show()

        main_df = spark.read \
                .option('branch', 'main') \
                .format('iceberg') \
                .table(f'{schema}.{table}')
        print('Output from main branch')
        main_df.show()
    
    @task.pyspark(conn_id="spark-conn", config_kwargs=spark_conf_dict)
    def fast_forward(spark:SparkSession, sc:SparkContext, schema=schema, table=table, branch=branch) -> None:
       spark.sql(f""" CALL system.fast_forward(table => '{schema}.{table}', branch => 'main', to => '{branch}')
        """)
       
    @task.pyspark(conn_id="spark-conn", config_kwargs=spark_conf_dict)
    def drop_audit_branch(spark:SparkSession, sc:SparkContext, schema=schema, table=table, branch=branch):
        spark.sql(f"""alter table {schema}.{table} drop branch {branch}""")
        spark.conf.unset('spark.wap.branch')
    
       
    run_sql = run_sql_stmt()
    delete_main_table = delete_table(current_ds=current_ds)
    create_branch = create_wap_branch()
    write_df = write_to_branch(current_ds=current_ds)
    fast_forward_main = fast_forward()
    drop_branch = drop_audit_branch()
    

    run_sql >> delete_main_table >> create_branch >> write_df >> fast_forward_main >> drop_branch

run_pyspark_wap_in_airflow()