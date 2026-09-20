import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# Recipient and SMTP credentials come from .env, not hardcoded here.
default_args = {
    "email_on_failure": True,
    "email": [os.getenv("ALERT_EMAIL_TO")],
}

with DAG(
    dag_id="fire_ingestion_dag",
    description="Ingest PetaBencana fire disaster reports into the GCS raw layer",
    start_date=datetime(2026, 1, 1),  # Airflow scheduling start_date, not the ingestion date range
    schedule="*/15 * * * *",  # each run processes at most 1 date via the GCS cursor; no-op once 2026-08-31 is done
    catchup=False,
    max_active_runs=1,  # runs would otherwise race on the same GCS state marker
    default_args=default_args,
    params={
        # Blank (default) -> auto next_date mode, reading the cursor in GCS.
        # Fill in both to force a one-off manual range instead - doesn't touch the cursor.
        "start_date": "",
        "end_date": "",
    },
) as dag:

    fetch_fire_reports = BashOperator(
        task_id="fetch_fire_reports_to_gcs",
        bash_command="python /opt/airflow/project/ingestion/fetch_batch_to_gcs.py",
        env={
            # "or ''" avoids Jinja rendering an unfilled param as the literal
            # string "None", which would wrongly look like a manual override.
            "PETABENCANA_ARCHIVE_START": "{{ params.start_date or '' }}",
            "PETABENCANA_ARCHIVE_END": "{{ params.end_date or '' }}",
        },
        append_env=True,  # keep the rest of .env alongside the two overrides above
    )

    # In auto mode this recomputes the same next_date and, only after a
    # successful merge, advances the GCS cursor so the next run moves on.
    load_raw_to_bigquery = BashOperator(
        task_id="load_raw_to_bigquery",
        bash_command="python /opt/airflow/project/batch/load_raw_to_bigquery.py",
        env={
            "PETABENCANA_ARCHIVE_START": "{{ params.start_date or '' }}",
            "PETABENCANA_ARCHIVE_END": "{{ params.end_date or '' }}",
        },
        append_env=True,
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="cd /opt/airflow/project/dbt && dbt run --select disaster_batch_staging",
    )

    dbt_dq = BashOperator(
        task_id="dbt_dq",
        bash_command="cd /opt/airflow/project/dbt && dbt run --select disaster_batch_staging_dq",
    )

    fetch_fire_reports >> load_raw_to_bigquery >> dbt_staging >> dbt_dq
