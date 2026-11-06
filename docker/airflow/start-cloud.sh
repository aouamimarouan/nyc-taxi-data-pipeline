
#!/usr/bin/env bash
set -euo pipefail

echo "Initializing Airflow database..."
airflow db migrate

echo "Ensuring Airflow admin user exists..."
airflow users create \
  --username "$AIRFLOW_ADMIN_USERNAME" \
  --password "$AIRFLOW_ADMIN_PASSWORD" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email "$AIRFLOW_ADMIN_EMAIL" \
  || echo "Admin user may already exist; check users if login fails."

echo "Starting Airflow scheduler..."
airflow scheduler &
SCHEDULER_PID=$!

echo "Starting Airflow webserver..."
airflow webserver &
WEBSERVER_PID=$!

# End the container if either critical service exits.
wait -n "$SCHEDULER_PID" "$WEBSERVER_PID"
exit 1
