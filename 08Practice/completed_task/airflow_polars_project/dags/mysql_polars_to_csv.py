from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import polars as pl
import pymysql
import os

MYSQL_CONN = {
    "host": "host.docker.internal",
    "user": "root",
    "password": "1234",
    "database": "training_dw",
    "port": 3306,
    "cursorclass": pymysql.cursors.DictCursor,
}


def extract_from_mysql():
    query = """
    SELECT c.city, o.status, o.amount_usd
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id;
    """
    conn = pymysql.connect(**MYSQL_CONN)
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
    finally:
        conn.close()


def transform_polars(ds, **kwargs):
    ti = kwargs["ti"]
    rows = ti.xcom_pull(task_ids="extract_from_mysql")

    df = pl.DataFrame(rows)

    metrics = (
        df.filter(pl.col("status") == "PAID")
        .group_by("city")
        .agg(
            [
                pl.count("status").alias("paid_orders_cnt"),
                pl.sum("amount_usd").alias("paid_revenue_usd"),
            ]
        )
        .with_columns(pl.lit(ds).alias("as_of_date"))
    )

    return metrics.to_dicts()


def write_csv(ds, **kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="transform_polars")
    df = pl.DataFrame(data)

    output_path = f"/opt/airflow/data/city_paid_metrics_{ds}.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write_csv(output_path)


with DAG(
    dag_id="mysql_polars_to_csv",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="extract_from_mysql", python_callable=extract_from_mysql
    )

    t2 = PythonOperator(task_id="transform_polars", python_callable=transform_polars)

    t3 = PythonOperator(task_id="write_csv", python_callable=write_csv)

    t1 >> t2 >> t3
