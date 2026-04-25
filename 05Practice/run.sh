#!/bin/bash

# Stop script execution if any command fails
set -e

echo "Activating virtual environment..."
source .venv/bin/activate

echo "Navigating to the target directory..."
cd 05Practice/completed_practice

echo "Running pyspark_solution.py..."
python pyspark_solution.py

echo "Script executed successfully."