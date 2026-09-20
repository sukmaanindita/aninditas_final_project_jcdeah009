import json
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from google.cloud import pubsub_v1
import requests
import os

# PetaBencana has no push/realtime endpoint, so "streaming" here means
# polling the same archive endpoint batch uses, with a short rolling window.
url = os.getenv("PETABENCANA_BASE_URL")
pubsub_topic_raw = os.getenv("PUBSUB_TOPIC_RAW")
stream_lookback_minutes = int(os.getenv("STREAM_LOOKBACK_MINUTES", "30"))
stream_end_override = os.getenv("STREAM_END")  # optional, for manual testing - Asia/Jakarta local time, no offset suffix

jakarta_timezone = ZoneInfo("Asia/Jakarta")


def build_time_window(stream_end_override: str, lookback_minutes: int):
    """STREAM_END, if set, is a naive WIB timestamp and gets converted to UTC. Otherwise the window ends now (UTC)."""
    if stream_end_override:
        naive_end_time = datetime.strptime(stream_end_override, "%Y-%m-%dT%H:%M:%S")
        jakarta_end_time = naive_end_time.replace(tzinfo=jakarta_timezone)
        end_time = jakarta_end_time.astimezone(timezone.utc)
    else:
        end_time = datetime.now(timezone.utc)

    start_time = end_time - timedelta(minutes=lookback_minutes)
    return start_time, end_time


def standardize_stream_url(url: str, start_time: datetime, end_time: datetime) -> str:
    """Same start/end query params as the batch endpoint, just a smaller window."""
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"{url}?start={start_str}&end={end_str}"


def fetch_reports(request_url: str):
    """Retries transient errors: HTTP 500/502/503/504, timeout, connection error."""
    request_headers = {"User-Agent": "jcdeah-009-final-project/1.0"}
    max_attempts = 3
    retry_delays_seconds = [2, 4]  # wait before attempt 2, then before attempt 3

    response = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(request_url, timeout=30, headers=request_headers)

            if response.status_code in (500, 502, 503, 504) and attempt < max_attempts:
                print(f"Got HTTP {response.status_code}, retrying (attempt {attempt}/{max_attempts})...")
                time.sleep(retry_delays_seconds[attempt - 1])
                continue

            break

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as retry_error:
            if attempt < max_attempts:
                print(f"{retry_error}, retrying (attempt {attempt}/{max_attempts})...")
                time.sleep(retry_delays_seconds[attempt - 1])
                continue
            raise

    response.raise_for_status()  # Raise an error for bad responses (non-retryable ones too)
    return response.json()


def filter_fire_reports(raw_json, start_time: datetime, end_time: datetime, polled_at: datetime):
    """Keep only disaster_type == "fire" (the API has no server-side filter). An empty poll is still a success."""
    geometries = (
        raw_json.get("result", {})
        .get("objects", {})
        .get("output", {})
        .get("geometries", [])
    )

    fire_reports = [
        geometry for geometry in geometries
        if geometry.get("properties", {}).get("disaster_type") == "fire"
    ]

    return {
        "polled_at": polled_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_start": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_end": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fire_reports": fire_reports,
    }


def publish_to_pubsub(pubsub_topic_raw: str, json_data: dict) -> str:
    """Waits for the publish to finish; raises on failure/timeout so the caller can exit non-zero."""
    publisher = pubsub_v1.PublisherClient()
    message_bytes = json.dumps(json_data).encode("utf-8")

    publish_future = publisher.publish(pubsub_topic_raw, message_bytes)
    message_id = publish_future.result(timeout=30)

    return message_id


def main():

    polled_at = datetime.now(timezone.utc)

    try:
        start_time, end_time = build_time_window(stream_end_override, stream_lookback_minutes)
        request_url = standardize_stream_url(url, start_time, end_time)
        print(f"Polling window: {start_time.strftime('%Y-%m-%dT%H:%M:%SZ')} to {end_time.strftime('%Y-%m-%dT%H:%M:%SZ')}")

        raw_json = fetch_reports(request_url)
        result = filter_fire_reports(raw_json, start_time, end_time, polled_at)

        # Always publish, even with 0 fire reports found - an empty poll still needs a record.
        message_id = publish_to_pubsub(pubsub_topic_raw, result)

        print(f"Poll successful: {len(result['fire_reports'])} fire report(s) found, published to {pubsub_topic_raw} (message_id={message_id})")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error during streaming poll: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
