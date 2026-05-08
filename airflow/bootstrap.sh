#!/usr/bin/env bash
# Bootstrap the Airflow scheduler container after `docker compose up -d`.
# Idempotent. Run from the airflow/ directory.
set -euo pipefail

CONTAINER="airflow-airflow-scheduler-1"
PYTHON="/home/airflow/.local/bin/python3"

echo "Waiting for scheduler to be ready..."
until docker exec "$CONTAINER" test -f /opt/airflow/airflow.cfg 2>/dev/null; do
    sleep 2
done

echo "Installing project dependencies..."
docker exec "$CONTAINER" \
    "$PYTHON" -m pip install --no-cache-dir --quiet \
    dbt-bigquery==1.11.1 \
    plaid-python==38.4.0 \
    google-cloud-bigquery==3.25.0 \
    google-cloud-storage==2.18.2 \
    python-dotenv==1.0.1

echo "Copying dbt profiles.yml..."
docker exec "$CONTAINER" mkdir -p /home/airflow/.dbt
docker cp ./config/profiles.yml "$CONTAINER":/home/airflow/.dbt/profiles.yml

echo "Setting up GCP key symlink..."
docker exec "$CONTAINER" mkdir -p /home/airflow/.gcp
docker exec "$CONTAINER" \
    ln -sf /opt/airflow/gcp/plaid-pipeline-sa-key.json /home/airflow/.gcp/plaid-pipeline-sa-key.json

echo ""
echo "Bootstrap complete."
echo "Airflow UI: http://localhost:8080  (login: airflow / airflow)"
