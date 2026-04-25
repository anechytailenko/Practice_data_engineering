#!/bin/bash

set -e

cd "$(dirname "$0")"

echo "Activating virtual environment..."
source ../.venv/bin/activate

echo "Setting up MySQL Database using existing test_db.sql..."
if [ -f "completed_task/test_db.sql" ]; then
    mysql -u root -p1234 -h localhost < completed_task/test_db.sql
    echo "MySQL database and products table successfully prepared."
else
    echo "Error: Could not find test_db.sql at completed_task/test_db.sql"
    exit 1
fi

echo "Navigating to the Airflow project directory..."
cd completed_task/airflow

echo "Checking for Astro CLI..."
if ! command -v astro &> /dev/null; then
    echo "Error: Astro CLI is not installed. Please run 'brew install astro' first."
    exit 1
fi

echo "Stopping and removing old containers..."
astro dev kill

echo "Starting Airflow..."
astro dev start

echo "Unpausing the DAG..."
astro dev run dags unpause mysql_to_duckdb_dag

echo "Triggering the DAG..."
astro dev run dags trigger mysql_to_duckdb_dag

echo "Waiting for 5 seconds for the pipeline to finish..."
sleep 5

echo "Navigating back to completed_task directory..."
cd ..

echo "Running verify_duckdb.py..."
python verify_duckdb.py

echo "Process completed successfully."