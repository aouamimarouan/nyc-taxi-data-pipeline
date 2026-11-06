
FROM apache/airflow:2.10.5-python3.12

# Preserve the dependencies from your existing project
COPY docker/airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

# Bake your existing DAGs into the image
COPY --chown=airflow:root dags/ /opt/airflow/dags/

# Cloud startup script
COPY --chown=airflow:root docker/airflow/start-cloud.sh /opt/airflow/start-cloud.sh

# Create writable folders for local task execution
RUN mkdir -p /opt/airflow/staging/nyc_taxi \
    /opt/airflow/logs \
    && chmod +x /opt/airflow/start-cloud.sh

CMD ["bash", "/opt/airflow/start-cloud.sh"]

