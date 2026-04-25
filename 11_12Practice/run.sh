#!/bin/bash

set -e

cd "$(dirname "$0")"

echo "Activating the main Practice virtual environment..."
source ../.venv/bin/activate

echo "Navigating to the dbt project directory..."
cd completed_practice/repo

echo "Running dbt model: mart_daily_orders..."
dbt run --select mart_daily_orders

echo "Running dbt model: mart_customers..."
dbt run --select mart_customers

echo "Running dbt snapshots..."
dbt snapshot

echo "All dbt tasks completed successfully."