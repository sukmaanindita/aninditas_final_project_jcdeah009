import os
import sys

import psycopg2
import requests

# Configuration

dim_region_api_url = os.getenv("DIM_REGION_API_URL")
dim_province_api_url = os.getenv("DIM_PROVINCE_API_URL")

postgres_host = os.getenv("POSTGRES_HOST")
postgres_port = os.getenv("POSTGRES_PORT")
postgres_db = os.getenv("POSTGRES_DB")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_schema = os.getenv("POSTGRES_SCHEMA_DIMENSION")

request_timeout = 30


def fetch_json(url: str):
    """Raises on a bad HTTP status or a non-array body - fail loudly rather than continue with wrong data."""
    response = requests.get(url, timeout=request_timeout)
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON array from {url}, got {type(data).__name__}")

    return data


def parse_provinces(raw_provinces):
    """A field missing from a record is left as None - never guessed."""
    rows = []
    for item in raw_provinces:
        rows.append({
            "id": item.get("id"),
            "name": item.get("name"),
            "alt_name": item.get("alt_name"),
            "latitude": item.get("latitude"),
            "longitude": item.get("longitude"),
        })
    return rows


def parse_regions(raw_regions):
    """Same as parse_provinces, plus province_id."""
    rows = []
    for item in raw_regions:
        rows.append({
            "id": item.get("id"),
            "province_id": item.get("province_id"),
            "name": item.get("name"),
            "alt_name": item.get("alt_name"),
            "latitude": item.get("latitude"),
            "longitude": item.get("longitude"),
        })
    return rows


def get_postgres_connection():
    return psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password,
    )


def create_dimension_tables(conn):
    """dim_region.province_id has a FK to dim_province.id, so dim_province must be created first."""
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {postgres_schema}")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {postgres_schema}.dim_province (
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                alt_name VARCHAR,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION
            )
        """)

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {postgres_schema}.dim_region (
                id VARCHAR PRIMARY KEY,
                province_id VARCHAR REFERENCES {postgres_schema}.dim_province(id),
                name VARCHAR,
                alt_name VARCHAR,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION
            )
        """)

    conn.commit()


def upsert_provinces(conn, rows):
    """Idempotent upsert by id - rerunning never creates duplicates."""
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(f"""
                INSERT INTO {postgres_schema}.dim_province (id, name, alt_name, latitude, longitude)
                VALUES (%(id)s, %(name)s, %(alt_name)s, %(latitude)s, %(longitude)s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    alt_name = EXCLUDED.alt_name,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude
            """, row)

    conn.commit()


def upsert_regions(conn, rows):
    """Same idempotent upsert pattern as upsert_provinces."""
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(f"""
                INSERT INTO {postgres_schema}.dim_region (id, province_id, name, alt_name, latitude, longitude)
                VALUES (%(id)s, %(province_id)s, %(name)s, %(alt_name)s, %(latitude)s, %(longitude)s)
                ON CONFLICT (id) DO UPDATE SET
                    province_id = EXCLUDED.province_id,
                    name = EXCLUDED.name,
                    alt_name = EXCLUDED.alt_name,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude
            """, row)

    conn.commit()


def main():

    if not dim_region_api_url or not dim_province_api_url:
        raise ValueError("DIM_REGION_API_URL and DIM_PROVINCE_API_URL environment variables are required.")

    print("[1/4] Fetching provinces.json and regencies.json...")
    raw_provinces = fetch_json(dim_province_api_url)
    raw_regions = fetch_json(dim_region_api_url)

    print("[2/4] Parsing province and region records...")
    province_rows = parse_provinces(raw_provinces)
    region_rows = parse_regions(raw_regions)

    print(f"[3/4] Connecting to PostgreSQL ({postgres_host}:{postgres_port}/{postgres_db})...")
    conn = get_postgres_connection()

    try:
        create_dimension_tables(conn)

        # Province first - dim_region.province_id references it.
        print(f"[4/4] Upserting {len(province_rows)} province(s) and {len(region_rows)} region(s)...")
        upsert_provinces(conn, province_rows)
        upsert_regions(conn, region_rows)
    finally:
        conn.close()

    print(
        f"Done. {len(province_rows)} province(s) and {len(region_rows)} region(s) "
        f"loaded into {postgres_schema}.dim_province / {postgres_schema}.dim_region."
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Failed to extract/load dimension data into PostgreSQL: {error}")
        sys.exit(1)
