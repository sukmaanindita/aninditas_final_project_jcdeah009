import sys
import os

import psycopg2
from google.cloud import bigquery

# Configuration

gcp_project_id = os.getenv("GCP_PROJECT_ID")
bq_dataset_dimension = os.getenv("BQ_DATASET_DIMENSION")
bq_table_dim_region = os.getenv("BQ_TABLE_DIM_REGION")
bq_table_dim_province = os.getenv("BQ_TABLE_DIM_PROVINCE")

postgres_host = os.getenv("POSTGRES_HOST")
postgres_port = os.getenv("POSTGRES_PORT")
postgres_db = os.getenv("POSTGRES_DB")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
postgres_schema = os.getenv("POSTGRES_SCHEMA_DIMENSION")


def get_postgres_connection():
    return psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password,
    )


def read_table(conn, columns, table_name: str):
    """Small reference tables - a full read is simple and cheap, no pagination needed."""
    column_list = ", ".join(columns)
    with conn.cursor() as cur:
        cur.execute(f"SELECT {column_list} FROM {postgres_schema}.{table_name}")
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def load_rows_into_bigquery(rows, target_table: str, schema):
    """WRITE_TRUNCATE - the table always ends up matching PostgreSQL exactly, reruns never accumulate duplicates."""
    if not rows:
        print(f"No rows read from PostgreSQL for {target_table}, skipping load.")
        return 0

    client = bigquery.Client(project=gcp_project_id)

    load_job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    load_job = client.load_table_from_json(rows, target_table, job_config=load_job_config)
    load_job.result()  # wait for the load to finish

    print(f"Loaded {len(rows)} row(s) into {target_table}")
    return len(rows)


def main():

    print(f"[1/3] Connecting to PostgreSQL ({postgres_host}:{postgres_port}/{postgres_db})...")
    conn = get_postgres_connection()

    try:
        print("[2/3] Reading dim_province and dim_region from PostgreSQL...")
        province_rows = read_table(conn, ["id", "name", "alt_name", "latitude", "longitude"], "dim_province")
        region_rows = read_table(conn, ["id", "province_id", "name", "alt_name", "latitude", "longitude"], "dim_region")
    finally:
        conn.close()

    # Province first, region second - matches the FK direction (BigQuery itself has no FK).
    print("[3/3] Loading into BigQuery...")

    province_table = f"{gcp_project_id}.{bq_dataset_dimension}.{bq_table_dim_province}"
    province_schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("alt_name", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT"),
        bigquery.SchemaField("longitude", "FLOAT"),
    ]
    load_rows_into_bigquery(province_rows, province_table, province_schema)

    region_table = f"{gcp_project_id}.{bq_dataset_dimension}.{bq_table_dim_region}"
    region_schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("province_id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("alt_name", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT"),
        bigquery.SchemaField("longitude", "FLOAT"),
    ]
    load_rows_into_bigquery(region_rows, region_table, region_schema)

    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Failed to load dimension data into BigQuery: {error}")
        sys.exit(1)
