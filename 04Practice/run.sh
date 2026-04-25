#!/bin/bash

# Stop script execution if any command fails
set -e

echo "Activating virtual environment..."
# Assuming .venv is in the Practice folder
source .venv/bin/activate

echo "Navigating to the target directory..."
cd 04Practice/completed_practice

echo "Running download_dataset.py..."
python download_dataset.py

echo "Running polars_solution.py..."
python polars_solution.py

echo "Running pandas_solution.py..."
python pandas_solution.py

echo "All scripts executed successfully."