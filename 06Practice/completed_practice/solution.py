import os
import pandas as pd
import sqlalchemy
import duckdb
from dotenv import load_dotenv

load_dotenv()


def run_etl():

    current_dir = os.path.dirname(os.path.abspath(__file__))
    duckdb_path = os.path.join(current_dir, "my_analytics.duckdb")

    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    host = os.getenv("MYSQL_HOST")
    port = os.getenv("MYSQL_PORT")
    db_name = os.getenv("MYSQL_DB")

    if host == "localhost":
        mysql_url = (
            f"mysql+pymysql://{user}:{password}@/{db_name}?unix_socket=/tmp/mysql.sock"
        )
    else:
        mysql_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}"

    engine = sqlalchemy.create_engine(mysql_url)

    # EXTRACT
    df = pd.read_sql("SELECT * FROM employees_data", engine)

    # TRANSFORM
    emp_rollup = (
        df.groupby("emp_no")
        .agg(
            birth_date=("birth_date", "min"),
            first_name=("first_name", "min"),
            last_name=("last_name", "min"),
            gender=("gender", "min"),
            hire_date=("hire_date", "min"),
            departments_worked=("dept_no", "nunique"),
            first_dept_no=("dept_no", "min"),
            first_dept_name=("dept_name", "min"),
            first_salary_from_date=("from_date", "min"),
            last_salary_to_date=("to_date", "max"),
            salary_records=("salary", "count"),
            min_salary=("salary", "min"),
            max_salary=("salary", "max"),
            avg_salary=("salary", "mean"),
        )
        .reset_index()
    )

    emp_rollup["salary_growth"] = emp_rollup["max_salary"] - emp_rollup["min_salary"]

    # LOAD
    with duckdb.connect(duckdb_path) as dd_conn:
        dd_conn.execute(
            "CREATE OR REPLACE TABLE emp_rollup AS SELECT * FROM emp_rollup"
        )

    print("\n--- Verification: Selecting from DuckDB ---")
    with duckdb.connect(duckdb_path) as dd_conn:
        result = dd_conn.execute("SELECT * FROM emp_rollup LIMIT 5").df()
        print(result)


if __name__ == "__main__":
    run_etl()
