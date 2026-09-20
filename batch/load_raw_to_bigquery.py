import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery
from google.cloud import storage

# Configuration

gcp_project_id = os.getenv("GCP_PROJECT_ID")
gcs_bucket_name = os.getenv("GCS_BUCKET_NAME")
gcs_raw_prefix = os.getenv("GCS_RAW_PREFIX")
bq_dataset_raw = os.getenv("BQ_DATASET_RAW")
bq_table_batch_raw = os.getenv("BQ_TABLE_BATCH_RAW")
get_data_start_date = os.getenv("PETABENCANA_ARCHIVE_START")  # manual override only - blank means auto next_date mode
get_data_end_date = os.getenv("PETABENCANA_ARCHIVE_END")
batch_historical_start_date = os.getenv("BATCH_HISTORICAL_START_DATE")
batch_historical_end_date = os.getenv("BATCH_HISTORICAL_END_DATE")

merge_sql_path = os.path.join(os.path.dirname(__file__), "..", "sql", "raw", "merge_disaster_batch_raw.sql")

# GCS marker of the last completed date. fetch_batch_to_gcs.py reads the same
# marker (read-only); only this script advances it, after a successful merge.
batch_state_blob_path = f"{gcs_raw_prefix}/_state/last_completed_date.txt"


def build_gcs_path(gcs_raw_prefix: str, report_date: str) -> str:
    """Matches the layout fetch_batch_to_gcs.py writes to."""
    year = report_date[0:4]
    month = report_date[5:7]
    return f"{gcs_raw_prefix}/year={year}/month={month}/{report_date}.json"


def read_json_from_gcs(gcs_bucket_name: str, gcs_path: str):
    """Returns the parsed dict, or None if the file doesn't exist."""
    client = storage.Client()
    bucket = client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_path)

    if not blob.exists():
        print(f"[{gcs_path}] File not found in GCS, skipping this date.")
        return None

    json_text = blob.download_as_text()
    return json.loads(json_text)


def extract_geometries(raw_json):
    """No "result" (no reports that day) is a valid outcome, not an error."""
    if not raw_json or "result" not in raw_json:
        return []

    return (
        raw_json.get("result", {})
        .get("objects", {})
        .get("output", {})
        .get("geometries", [])
    )


def build_record_key(pkey, created_at, url) -> str:
    """SHA256(pkey|created_at), or SHA256(no_pkey|url|created_at) when pkey is NULL."""
    if pkey:
        raw_key = f"{pkey}|{created_at}"
    else:
        raw_key = f"no_pkey|{url}|{created_at}"

    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def build_raw_rows(geometries, source_file: str):
    """No transformation here - full geometry kept as-is, every disaster_type included (fire filtering happens in staging)."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    rows = []

    for geometry in geometries:
        properties = geometry.get("properties", {})
        pkey = properties.get("pkey")
        created_at = properties.get("created_at")
        disaster_type = properties.get("disaster_type")
        url = properties.get("url")

        row = {
            "record_key": build_record_key(pkey, created_at, url),
            "pkey": pkey,
            "created_at": created_at,
            "disaster_type": disaster_type,
            "raw_geometry": json.dumps(geometry),
            "source_file": source_file,
            "ingested_at": ingested_at,
        }
        rows.append(row)

    return rows


def merge_rows_into_bigquery(rows, report_date: str) -> int:
    """MERGE by record_key - re-running the same date is safe, no duplicate rows."""
    if not rows:
        print(f"[{report_date}] No reports for this date, nothing to load.")
        return 0

    client = bigquery.Client(project=gcp_project_id)

    target_table = f"{gcp_project_id}.{bq_dataset_raw}.{bq_table_batch_raw}"
    temp_table = f"{gcp_project_id}.{bq_dataset_raw}._tmp_disaster_batch_raw"

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
    query_job.result()  # wait for the merge to finish

    print(f"[{report_date}] Merged {len(rows)} row(s) into {target_table}")
    return len(rows)


def load_one_date_to_raw(report_date: str) -> int:
    """Read from GCS, parse, build rows, merge into BigQuery raw."""
    gcs_path = build_gcs_path(gcs_raw_prefix, report_date)
    raw_json = read_json_from_gcs(gcs_bucket_name, gcs_path)
    geometries = extract_geometries(raw_json)

    source_file = f"gs://{gcs_bucket_name}/{gcs_path}"
    rows = build_raw_rows(geometries, source_file)

    return merge_rows_into_bigquery(rows, report_date)


def get_last_completed_date(gcs_bucket_name: str, state_blob_path: str):
    """Read the last completed date from the GCS state marker, or None if it doesn't exist yet."""
    client = storage.Client()
    bucket = client.bucket(gcs_bucket_name)
    blob = bucket.blob(state_blob_path)

    if not blob.exists():
        return None

    return blob.download_as_text().strip()


def get_next_date_to_process(gcs_bucket_name: str, state_blob_path: str, historical_start: str, historical_end: str):
    """The day after the last completed date, or historical_start if nothing is done yet. None once the range is finished."""
    last_completed_date = get_last_completed_date(gcs_bucket_name, state_blob_path)

    if last_completed_date is None:
        next_date = datetime.strptime(historical_start, "%Y-%m-%d")
    else:
        next_date = datetime.strptime(last_completed_date, "%Y-%m-%d") + timedelta(days=1)

    end_date = datetime.strptime(historical_end, "%Y-%m-%d")
    if next_date > end_date:
        return None

    return next_date.strftime("%Y-%m-%d")


def mark_date_completed(gcs_bucket_name: str, state_blob_path: str, report_date: str):
    """Only call after a successful merge - if this is never reached, the next run retries the same date."""
    client = storage.Client()
    bucket = client.bucket(gcs_bucket_name)
    blob = bucket.blob(state_blob_path)
    blob.upload_from_string(report_date)


def main():

    if get_data_start_date and get_data_end_date:
        # Manual override: loop the whole range, don't touch the next_date cursor.
        start_date = datetime.strptime(get_data_start_date, "%Y-%m-%d")
        end_date = datetime.strptime(get_data_end_date, "%Y-%m-%d")

        current_date = start_date
        total_rows = 0
        while current_date <= end_date:
            report_date = current_date.strftime("%Y-%m-%d")
            total_rows += load_one_date_to_raw(report_date)
            current_date = current_date + timedelta(days=1)

        print(f"Done. Total rows merged: {total_rows}")

    else:
        # No override: auto mode, process only the next unprocessed date.
        next_date = get_next_date_to_process(gcs_bucket_name, batch_state_blob_path, batch_historical_start_date, batch_historical_end_date)

        if next_date is None:
            print(f"All historical dates ({batch_historical_start_date} to {batch_historical_end_date}) already processed. Nothing to do.")
            return

        total_rows = load_one_date_to_raw(next_date)
        mark_date_completed(gcs_bucket_name, batch_state_blob_path, next_date)

        print(f"Done. Marked {next_date} as completed. Total rows merged: {total_rows}")


if __name__ == "__main__":
    main()
