#!/bin/bash

set -e

cd "$(dirname "$0")"

echo "Activating virtual environment..."
source ../.venv/bin/activate

echo "Downloading data via downloader.py..."
python ./completed_practice/downloader.py

echo "Executing DuckDB analysis..."
duckdb :memory: ".read ./completed_practice/nike_analysis.sql"

echo "Process completed successfully."