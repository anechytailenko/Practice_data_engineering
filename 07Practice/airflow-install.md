### Airflow

#### How to install Airflow via Astronomer

Install Docker

Docker is a tool that lets you package an application and everything it needs into a container so it runs the same way on any computer.

Official instruction - https://www.astronomer.io/docs/astro/cli/install-cli

#### MacOS
To install the latest version of the Astro CLI, run the following command:

```brew install astro```

To verify that the correct Astro CLI version was installed, run:
```
astro version
```

#### Astro CLI commands:

Initializes project with Airflow 3.1.3:
```
astro dev init --airflow-version=3.1.3
```

Start Airflow:
```
astro dev start
```

Restart Airflow:
```
astro dev restart
```

#### Windows
Follow instructions - https://www.astronomer.io/docs/astro/cli/install-cli

#### After installation

1. Add [docker-compose.override.yml](airflow3%2Fdocker-compose.override.yml) to your initialized project.
2. Add [mysql_to_duckdb_dag.py](airflow3%2Fdags%2Fmysql_to_duckdb_dag.py) to your initialized project.
3. Copy paste [requirements.txt](airflow3%2Frequirements.txt).
4. Run:
```
astro dev restart
```

### MySQL Preparations

```
-- Create database
CREATE DATABASE test_db;
USE test_db;

-- Create table
CREATE TABLE products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255),
    price DOUBLE
);

-- Insert some sample data
INSERT INTO products (name, price) VALUES 
    ('Laptop', 1200.00),
    ('Phone', 600.00),
    ('Tablet', 300.00);
```

