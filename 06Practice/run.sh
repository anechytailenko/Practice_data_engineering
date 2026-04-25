#!/bin/bash

# Stop script execution if any command fails
set -e

echo "Activating virtual environment..."
source .venv/bin/activate

echo "Navigating to directory 06Practice/completed_practice..."
cd 06Practice/completed_practice

# Check for the existence of example.env just in case
if [ ! -f "example.env" ]; then
    echo "Error: example.env file not found!"
    exit 1
fi

echo "Setting up the environment file (.env)..."
# If the .env file does not exist, copy it from example.env
if [ ! -f ".env" ]; then
    cp example.env .env
    echo "  -> .env file successfully created from example.env."
else
    echo "  -> .env file already exists. Using it."
fi

echo "Loading environment variables..."
# Load variables from .env so the bash script can use them
set -a
source .env
set +a

echo "Running SQL script (solution.sql)..."
# Use the variables we just loaded from the .env file
mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" -h "$MYSQL_HOST" -P "$MYSQL_PORT" "$MYSQL_DB" -t < solution.sql

echo "Running Python script (solution.py)..."
python solution.py

echo "All steps completed successfully."