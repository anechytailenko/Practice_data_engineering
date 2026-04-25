### Airflow DAG (3 tasks) MySQL → Polars → CSV with XCom
_1 point_

Build an Airflow DAG with exactly 3 tasks:

1. Extract aggregated data from MySQL 
2. Transform using Polars 
3. Write results to CSV

Use [XCom](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html) to pass data between tasks.

#### MySQL setup (run once)
Create DB + tables
```
CREATE DATABASE IF NOT EXISTS training_dw;
USE training_dw;

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
  customer_id INT PRIMARY KEY AUTO_INCREMENT,
  full_name   VARCHAR(100) NOT NULL,
  city        VARCHAR(80)  NOT NULL,
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
  order_id     INT PRIMARY KEY AUTO_INCREMENT,
  customer_id  INT NOT NULL,
  order_date   DATE NOT NULL,
  amount_usd   DECIMAL(10,2) NOT NULL,
  status       VARCHAR(20) NOT NULL,
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
```
Insert sample data

```
USE training_dw;

INSERT INTO customers (full_name, city) VALUES
('Alice Johnson', 'Kyiv'),
('Bob Smith', 'Lviv'),
('Chris Lee', 'Kyiv'),
('Daria Ivanova', 'Odesa');

INSERT INTO orders (customer_id, order_date, amount_usd, status) VALUES
(1, '2026-01-05', 120.50, 'PAID'),
(1, '2026-01-20',  75.00, 'PAID'),
(2, '2026-01-11', 200.00, 'CANCELLED'),
(2, '2026-02-01',  50.00, 'PAID'),
(3, '2026-02-10',  99.99, 'PAID'),
(4, '2026-02-15', 300.00, 'PAID');
```

#### DAG requirements

Exactly 3 tasks:

1. extract_from_mysql: run query, return rows via XCom (small) or write temp file + XCom path (preferred)
2. transform_polars: load into Polars, add as_of_date ({{ ds }}), ensure schema
3. write_csv: write CSV to /opt/airflow/data/city_paid_metrics_{{ ds }}.csv

#### Minimal Polars usage (must be in task 2)

1. Create pl.DataFrame from extracted rows
2. Add column as_of_date
3. Write CSV (either in task 3 or return DF and write in task 3)

Example transform logic (conceptual):
```
df = pl.DataFrame(rows, schema=[...])
df = df.with_columns(pl.lit(ds).alias("as_of_date"))
```

#### Deliverables

`dags/mysql_polars_to_csv.py` (3 tasks, XCom used)

Output CSV in /opt/airflow/data/ with expected columns:
```
city, paid_orders_cnt, paid_revenue_usd, as_of_date
```