#!/bin/bash

set -e

# Change directory to where the script (and task.sql) is located
cd "$(dirname "$0")"

LOCAL_TASK=".completed_practice/task.sql"

echo "Checking for task.sql..."
if [ ! -f "$LOCAL_TASK" ]; then
    echo "Error: task.sql not found in the 03Practice directory."
    exit 1
fi
echo "Found task.sql in the current directory."

if ! command -v git &> /dev/null; then
    echo "Git is not installed. Please install it."
    exit 1
fi

if [ ! -d "test_db" ]; then
    echo "Cloning test_db..."
    git clone https://github.com/datacharmer/test_db.git
else
    echo "test_db directory already exists. Skipping clone."
fi

# Request password once and export it as an environment variable
echo -n "Enter MySQL root password: "
read -s MYSQL_PWD
echo ""
export MYSQL_PWD

echo "Importing employees database..."
cd test_db
mysql -u root < employees.sql
cd ..

echo "Executing task.sql and fetching snapshot..."
mysql -u root -t employees < "$LOCAL_TASK"

# Clear the password variable for security
unset MYSQL_PWD

echo "Process completed."