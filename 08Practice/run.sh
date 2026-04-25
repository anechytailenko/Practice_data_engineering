#!/bin/bash

# Stop script execution if any command fails
set -e

# Change directory to where the script is located (inside the 08Practice folder)
cd "$(dirname "$0")"

echo "Activating virtual environment..."
# Assuming the .venv is in the main Practice folder (one level up)
source ../.venv/bin/activate

echo "Setting up MySQL Database using create_db.sql..."
if [ -f "completed_task/create_db.sql" ]; then
    mysql -u root -p1234 -h localhost < completed_task/create_db.sql
    echo "MySQL database successfully prepared."
else
    echo "Error: Could not find create_db.sql at completed_task/create_db.sql"
    exit 1
fi

echo "Navigating to the Airflow project directory..."
cd completed_task/airflow_polars_project

echo "Checking for Astro CLI..."
if ! command -v astro &> /dev/null; then
    echo "Error: Astro CLI is not installed. Please run 'brew install astro' first."
    exit 1
fi

echo "Stopping and removing old Airflow containers..."
astro dev kill

echo "Building and starting fresh Airflow containers..."
astro dev start

# Note: We are assuming the DAG ID inside your python file is exactly "mysql_polars_to_csv". 
# If it is named something else inside the python code (e.g., "polars_etl"), change it in the two lines below.
echo "Unpausing the DAG..."
astro dev run dags unpause mysql_polars_to_csv

echo "Triggering the DAG..."
astro dev run dags trigger mysql_polars_to_csv

echo "Waiting 10 seconds for the pipeline to extract, transform, and save the CSV..."
sleep 10

echo "Navigating back to completed_task directory..."
cd ..

echo "Running verify_csv.py to check the output..."
python verify_csv.py

echo "Process completed successfully."