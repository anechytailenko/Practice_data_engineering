import duckdb

DB_PATH = "./airflow/data/sales.duckdb"


def verify_data():
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)

        df = conn.execute("SELECT * FROM products").df()
        print(df)

        conn.close()
    except Exception as e:
        print("Error - airflow probably did not do the task")


if __name__ == "__main__":
    verify_data()
