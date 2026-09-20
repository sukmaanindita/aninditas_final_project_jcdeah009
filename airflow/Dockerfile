FROM apache/airflow:2.10.4-python3.12

# Step 1: Copy the ingestion-only dependency list
COPY requirements-airflow.txt /requirements-airflow.txt

# Step 2: Install dependencies needed by ingestion/fetch_batch_to_gcs.py
RUN pip install --no-cache-dir -r /requirements-airflow.txt
