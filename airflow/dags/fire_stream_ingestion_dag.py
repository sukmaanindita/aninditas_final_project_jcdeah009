import os
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

# Same alert config as fire_ingestion_dag.py.
default_args = {
    "email_on_failure": True,
    "email": [os.getenv("ALERT_EMAIL_TO")],
}

with DAG(
    dag_id="fire_stream_ingestion_dag",
    description="Poll PetaBencana fire reports and stream them to BigQuery via Pub/Sub",
    start_date=datetime(2026, 1, 1),  # Airflow scheduling start_date, not a data date range
    schedule="*/5 * * * *",  # every 5 minutes
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        # Param(...) instead of plain strings just so the description shows
        # up in the Airflow "Trigger DAG w/ config" form.
        "stream_end": Param(
            "",
            type="string",
            description="End time in WIB (Asia/Jakarta). Format: YYYY-MM-DDTHH:MM:SS. Do not add timezone suffix. Example: 2026-09-19T16:00:00",
        ),
        "stream_lookback_minutes": Param(
            "10",
            type="string",
            description="Lookback duration in minutes from stream_end. Example: 120 = query 2 hours before stream_end.",
        ),
    },
) as dag:

    publish_fire_reports_to_pubsub = BashOperator(
        task_id="publish_fire_reports_to_pubsub",
        bash_command="python3 /opt/airflow/project/ingestion/publish_stream_to_pubsub.py",
        env={
            "STREAM_END": "{{ params.stream_end }}",
            "STREAM_LOOKBACK_MINUTES": "{{ params.stream_lookback_minutes }}",
        },
        append_env=True,  # keep the rest of .env alongside the two overrides above
    )

    # Pulls whatever is available right now; only acks after a successful MERGE.
    consume_fire_reports_to_bigquery = BashOperator(
        task_id="consume_fire_reports_to_bigquery",
        bash_command="python3 /opt/airflow/project/stream/load_stream_to_bigquery.py",
    )

    dbt_stream_staging = BashOperator(
        task_id="dbt_stream_staging",
        bash_command="cd /opt/airflow/project/dbt && dbt build --select disaster_stream_staging",
    )

    dbt_stream_dq = BashOperator(
        task_id="dbt_stream_dq",
        bash_command="cd /opt/airflow/project/dbt && dbt build --select disaster_stream_staging_dq",
    )

    publish_fire_reports_to_pubsub >> consume_fire_reports_to_bigquery >> dbt_stream_staging >> dbt_stream_dq
