from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import duckdb
import pymysql  # MySQL connector

# Define database credentials
MYSQL_CONFIG = {
    "host": "host.docker.internal",  # Connect to MySQL from inside Docker
    "user": "root",
    "password": "1234",
    "database": "test_db",
    "port": 3306,
}

DUCKDB_PATH = "/usr/local/airflow/data/sales.duckdb"  # Path for DuckDB file


# Function to extract data from MySQL
def etl():

    # Extract data
    conn = pymysql.connect(**MYSQL_CONFIG)
    query = "SELECT id, name, price FROM products;"  # Select some data
    df = pd.read_sql(query, conn)
    conn.close()
    print("✅ Data extracted from MySQL.")

    # Transform data
    df["price"] = df["price"] * 1000.1  # Increase price by 10%
    print("✅ Data transformed (price increased by 10%).")

    # Loading data into DuckDB
    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute(
        """
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER,
                name STRING,
                price DOUBLE
            );
        """
    )

    conn.execute("INSERT INTO products SELECT * FROM df")
    conn.close()

    print("✅ Transformed data inserted into DuckDB.")


# Define Airflow DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 1),
    "catchup": False,
}

dag = DAG(
    "mysql_to_duckdb_dag",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
)

# Define tasks

etl = PythonOperator(task_id="transform_data", python_callable=etl, dag=dag)

# Task dependencies
etl
