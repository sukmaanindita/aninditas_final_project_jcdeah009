import hashlib
import json
import os
import sys
from datetime import datetime, timezone

import jsonschema
from google.cloud import bigquery
from google.cloud import pubsub_v1

# Configuration

gcp_project_id = os.getenv("GCP_PROJECT_ID")
bq_dataset_raw = os.getenv("BQ_DATASET_RAW")
bq_table_stream_raw = os.getenv("BQ_TABLE_STREAM_RAW")
pubsub_subscription_raw = os.getenv("PUBSUB_SUBSCRIPTION_RAW")

# One pull, process, ack, exit - not a continuous daemon.
max_messages_per_pull = 10

# Reused as-is from the batch raw loader - already generic on target/temp table.
merge_sql_path = os.path.join(os.path.dirname(__file__), "..", "sql", "raw", "merge_disaster_batch_raw.sql")

# Contract for the envelope published by ingestion/publish_stream_to_pubsub.py.
event_schema_path = os.path.join(os.path.dirname(__file__), "..", "schemas", "event_schema.json")
with open(event_schema_path) as event_schema_file:
    event_schema = json.load(event_schema_file)


def pull_messages(pubsub_subscription_raw: str, max_messages: int):
    """A single bounded pull, not a loop - returns whatever is available right now."""
    subscriber = pubsub_v1.SubscriberClient()
    response = subscriber.pull(
        request={"subscription": pubsub_subscription_raw, "max_messages": max_messages},
        timeout=30,
    )
    subscriber.close()
    return response.received_messages


def validate_event_schema(raw_json):
    """Raises jsonschema.exceptions.ValidationError if the envelope doesn't match schemas/event_schema.json - a structural mismatch should fail the task, not be treated as an empty event."""
    jsonschema.validate(instance=raw_json, schema=event_schema)


def parse_message(received_message, pubsub_subscription_raw: str):
    """Returns None on invalid UTF-8/JSON - a malformed message is left unacked, never silently processed."""
    try:
        payload_text = received_message.message.data.decode("utf-8")
        raw_json = json.loads(payload_text)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        print(f"[ack_id={received_message.ack_id}] Malformed message, skipping (NOT acked): {error}")
        return None

    validate_event_schema(raw_json)

    fire_reports = extract_fire_reports(raw_json)
    source_file = f"pubsub:{pubsub_subscription_raw}#{received_message.message.message_id}"
    return fire_reports, source_file


def extract_fire_reports(raw_json):
    """Payload shape is {polled_at, window_start, window_end, fire_reports: [...]} from publish_stream_to_pubsub.py."""
    if not raw_json:
        return []

    return raw_json.get("fire_reports", [])


def build_record_key(pkey, created_at, url) -> str:
    """Same formula as batch/load_raw_to_bigquery.py: SHA256(pkey|created_at), or SHA256(no_pkey|url|created_at) when pkey is NULL."""
    if pkey:
        raw_key = f"{pkey}|{created_at}"
    else:
        raw_key = f"no_pkey|{url}|{created_at}"

    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def build_raw_rows(fire_reports, source_file: str):
    """Same schema as disaster_batch_raw. No transformation - full report kept as-is in raw_geometry."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    rows = []

    for report in fire_reports:
        properties = report.get("properties", {})
        pkey = properties.get("pkey")
        created_at = properties.get("created_at")
        disaster_type = properties.get("disaster_type")
        url = properties.get("url")

        row = {
            "record_key": build_record_key(pkey, created_at, url),
            "pkey": pkey,
            "created_at": created_at,
            "disaster_type": disaster_type,
            "raw_geometry": json.dumps(report),
            "source_file": source_file,
            "ingested_at": ingested_at,
        }
        rows.append(row)

    return rows


def merge_rows_into_bigquery(rows) -> int:
    """Same MERGE mechanism as the batch loader - overlapping/duplicate reports resolve to the same record_key, no duplicates."""
    if not rows:
        print("No fire reports in this pulled batch, nothing to load.")
        return 0

    client = bigquery.Client(project=gcp_project_id)

    target_table = f"{gcp_project_id}.{bq_dataset_raw}.{bq_table_stream_raw}"
    temp_table = f"{gcp_project_id}.{bq_dataset_raw}._tmp_disaster_stream_raw"

    load_job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("record_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("pkey", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("disaster_type", "STRING"),
            bigquery.SchemaField("raw_geometry", "STRING"),
            bigquery.SchemaField("source_file", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    load_job = client.load_table_from_json(rows, temp_table, job_config=load_job_config)
    load_job.result()  # wait for the load to finish

    with open(merge_sql_path) as sql_file:
        merge_sql = sql_file.read()

    merge_sql = merge_sql.replace("{{ target_table }}", target_table)
    merge_sql = merge_sql.replace("{{ temp_table }}", temp_table)

    query_job = client.query(merge_sql)
    query_job.result()  # wait for the merge to finish - raises on failure

    print(f"Merged {len(rows)} row(s) into {target_table}")
    return len(rows)


def ack_messages(pubsub_subscription_raw: str, ack_ids):
    """Only call after a successful MERGE - otherwise messages stay unacked and Pub/Sub redelivers them."""
    if not ack_ids:
        return

    subscriber = pubsub_v1.SubscriberClient()
    subscriber.acknowledge(
        request={"subscription": pubsub_subscription_raw, "ack_ids": ack_ids}
    )
    subscriber.close()


def main():

    if not pubsub_subscription_raw:
        raise ValueError("PUBSUB_SUBSCRIPTION_RAW environment variable is required.")

    received_messages = pull_messages(pubsub_subscription_raw, max_messages_per_pull)

    if not received_messages:
        print("No messages available to pull. Nothing to process.")
        return

    print(f"Pulled {len(received_messages)} message(s).")

    all_rows = []
    ack_ids = []
    for received_message in received_messages:
        parsed = parse_message(received_message, pubsub_subscription_raw)
        if parsed is None:
            continue

        fire_reports, source_file = parsed
        rows = build_raw_rows(fire_reports, source_file)
        all_rows.extend(rows)
        ack_ids.append(received_message.ack_id)

    # One MERGE for the whole pulled batch - if it raises, nothing gets acked and Pub/Sub redelivers.
    try:
        total_rows = merge_rows_into_bigquery(all_rows)
    except Exception as error:
        print(f"BigQuery MERGE failed, messages will NOT be acked (will be redelivered): {error}")
        sys.exit(1)

    ack_messages(pubsub_subscription_raw, ack_ids)
    print(f"Acked {len(ack_ids)} message(s) after successful MERGE.")

    print(f"Done. Total rows merged: {total_rows}")


if __name__ == "__main__":
    main()
