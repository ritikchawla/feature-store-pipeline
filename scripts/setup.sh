#!/bin/bash

# Exit on error
set -e

echo "Setting up Feature Store Pipeline environment..."

# Create Python virtual environment
echo "Creating Python virtual environment..."
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create necessary directories if they don't exist
echo "Setting up directory structure..."
mkdir -p data/{raw,processed}
mkdir -p logs

# Initialize Feast repository
echo "Initializing Feast repository..."
cd feast/feature_repo
feast init feature_store_pipeline --minimal
cd ../..

# Set up environment variables
echo "Setting up environment variables..."
cat > .env << EOL
AIRFLOW_HOME=$(pwd)/airflow
SPARK_HOME=/usr/local/spark
FEAST_REPO_PATH=$(pwd)/feast/feature_repo
EOL

# Initialize Airflow
echo "Initializing Airflow..."
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow users create \
    --username admin \
    --firstname admin \
    --lastname user \
    --role Admin \
    --email admin@example.com \
    --password admin

# Create Airflow connections
echo "Setting up Airflow connections..."
airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'local' \
    --conn-extra '{"spark.driver.memory": "4g"}'

# Create test data directory
echo "Creating test data directory..."
mkdir -p data/raw/transactions

echo "Setup completed successfully!"
echo "
Next steps:
1. Start Redis server: redis-server
2. Start Airflow webserver: airflow webserver
3. Start Airflow scheduler: airflow scheduler
4. Access Airflow UI at http://localhost:8080 (username: admin, password: admin)
5. Register feature definitions: cd feast/feature_repo && feast apply
"