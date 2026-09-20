import json
import sys
import time
from datetime import datetime, timedelta
from google.cloud import storage
import requests
import os

url = os.getenv("PETABENCANA_BASE_URL")
get_data_start_date = os.getenv("PETABENCANA_ARCHIVE_START")  # manual override only - blank means auto next_date mode
get_data_end_date = os.getenv("PETABENCANA_ARCHIVE_END")
gcs_bucket = os.getenv("GCS_BUCKET_NAME")
gcs_raw_prefix = os.getenv("GCS_RAW_PREFIX")
batch_historical_start_date = os.getenv("BATCH_HISTORICAL_START_DATE")
batch_historical_end_date = os.getenv("BATCH_HISTORICAL_END_DATE")

# GCS marker of the last completed date - read-only here, load_raw_to_bigquery.py owns writing it.
batch_state_blob_path = f"{gcs_raw_prefix}/_state/last_completed_date.txt"


def standardize_url(url: str, report_date: str) -> str:
    """Build the archive API URL for a full day (00:00:00Z to 23:59:59Z)."""
    start_date_timestamp = report_date + "T00:00:00Z"
    end_date_timestamp = report_date + "T23:59:59Z"

    standardized_url = f"{url}?start={start_date_timestamp}&end={end_date_timestamp}"
    return standardized_url


def build_gcs_path(gcs_raw_prefix: str, report_date: str) -> str:
    """Example: raw/petabencana/fire/year=2026/month=09/2026-09-01.json"""
    year = report_date[0:4]
    month = report_date[5:7]

    gcs_path = f"{gcs_raw_prefix}/year={year}/month={month}/{report_date}.json"
    return gcs_path


def fetch_to_gcs(url: str, gcs_bucket: str, gcs_raw_prefix: str, report_date: str):

    try:
        # Retry transient errors only: HTTP 429/500/502/503/504, timeout, connection error.
        request_headers = {"User-Agent": "jcdeah-009-final-project/1.0"}
        max_attempts = 3
        retry_delays_seconds = [2, 4]  # wait before attempt 2, then before attempt 3

        response = None
        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.get(
                    standardize_url(url, report_date),
                    timeout=30,
                    headers=request_headers,
                )

                # Retry only for these specific transient HTTP status codes
                if response.status_code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                    print(f"[{report_date}] Got HTTP {response.status_code}, retrying (attempt {attempt}/{max_attempts})...")
                    time.sleep(retry_delays_seconds[attempt - 1])
                    continue

                break

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as retry_error:
                if attempt < max_attempts:
                    print(f"[{report_date}] {retry_error}, retrying (attempt {attempt}/{max_attempts})...")
                    time.sleep(retry_delays_seconds[attempt - 1])
                    continue
                raise

        response.raise_for_status()

        gcs_path = build_gcs_path(gcs_raw_prefix, report_date)

        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob = bucket.blob(gcs_path)

        blob.upload_from_string(response.content)  # overwrites if this date was already fetched

        print(f"[{report_date}] Data successfully fetched and uploaded to gs://{gcs_bucket}/{gcs_path}")
        return True

    except requests.exceptions.RequestException as e:
        print(f"[{report_date}] Error fetching data from {url}: {e}")
        return False
    except Exception as e:
        print(f"[{report_date}] Error uploading data to GCS: {e}")
        return False


def get_last_completed_date(gcs_bucket: str, state_blob_path: str):
    """Read the last completed date from the GCS state marker, or None if it doesn't exist yet."""
    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(state_blob_path)

    if not blob.exists():
        return None

    return blob.download_as_text().strip()


def get_next_date_to_process(gcs_bucket: str, state_blob_path: str, historical_start: str, historical_end: str):
    """The day after the last completed date, or historical_start if nothing is done yet. None once the range is finished."""
    last_completed_date = get_last_completed_date(gcs_bucket, state_blob_path)

    if last_completed_date is None:
        next_date = datetime.strptime(historical_start, "%Y-%m-%d")
    else:
        next_date = datetime.strptime(last_completed_date, "%Y-%m-%d") + timedelta(days=1)

    end_date = datetime.strptime(historical_end, "%Y-%m-%d")
    if next_date > end_date:
        return None

    return next_date.strftime("%Y-%m-%d")


def main():

    if get_data_start_date and get_data_end_date:
        # Manual override: loop through every date in the given range.
        start_date = datetime.strptime(get_data_start_date, "%Y-%m-%d")
        end_date = datetime.strptime(get_data_end_date, "%Y-%m-%d")

        failed_dates = []
        current_date = start_date
        while current_date <= end_date:
            report_date = current_date.strftime("%Y-%m-%d")

            is_success = fetch_to_gcs(url, gcs_bucket, gcs_raw_prefix, report_date)

            if not is_success:
                failed_dates.append(report_date)

            current_date = current_date + timedelta(days=1)

        if failed_dates:
            print(f"Failed to process {len(failed_dates)} date(s): {failed_dates}")
            sys.exit(1)

    else:
        # No override: auto mode, fetch only the next unprocessed date.
        next_date = get_next_date_to_process(gcs_bucket, batch_state_blob_path, batch_historical_start_date, batch_historical_end_date)

        if next_date is None:
            print(f"All historical dates ({batch_historical_start_date} to {batch_historical_end_date}) already processed. Nothing to do.")
            return

        is_success = fetch_to_gcs(url, gcs_bucket, gcs_raw_prefix, next_date)

        if not is_success:
            print(f"Failed to process {next_date}")
            sys.exit(1)


if __name__ == "__main__":
    main()
