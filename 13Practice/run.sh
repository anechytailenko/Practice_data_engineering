#!/bin/bash

# Stop script execution if any command fails
set -e

# Change directory to where the script is located (inside the 13Practice folder)
cd "$(dirname "$0")"

echo "Checking for native MinIO and MinIO Client (mc)..."
if ! command -v minio &> /dev/null; then
    echo "Installing MinIO via Homebrew..."
    brew install minio/stable/minio
fi

if ! command -v mc &> /dev/null; then
    echo "Installing MinIO Client (mc) via Homebrew..."
    brew install minio/stable/mc
fi

echo "Starting local MinIO server in the background..."
# Create a temporary directory for MinIO to store data
mkdir -p /tmp/minio_data

# Start MinIO on port 9002 and default console port 9003
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin minio server /tmp/minio_data --address ":9002" --console-address ":9003" > /tmp/minio.log 2>&1 &
MINIO_PID=$!

echo "Waiting for MinIO to boot up (5 seconds)..."
sleep 5

echo "Configuring MinIO Client and creating the bucket..."
mc alias set localminio http://127.0.0.1:9002 minioadmin minioadmin

mc mb localminio/my-dbt-source || true

echo "Uploading my_data.csv to the MinIO bucket..."
mc cp completed_practice/dbt/my_data.csv localminio/my-dbt-source/my_data.csv

echo "Activating virtual environment..."
if [ -f "../.venv/bin/activate" ]; then
    source ../.venv/bin/activate
else
    echo "Warning: Virtual environment not found at ../.venv/bin/activate."
fi

echo "Navigating to the dbt project directory..."
cd completed_practice
cd dbt

echo "Running dbt show for my_model..."
dbt show --select my_model

echo "Cleaning up..."


kill $MINIO_PID
echo "Process completed successfully. MinIO server shut down."