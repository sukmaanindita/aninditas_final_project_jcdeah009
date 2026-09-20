#!/usr/bin/env bash
set -euo pipefail

# Runs the batch/stream/dimension pipelines using the same scripts and dbt
# selectors as the Airflow DAGs, without needing Airflow running.
#
# Usage:
#   ./auto_command.sh batch [start_date end_date]
#   ./auto_command.sh stream [lookback_minutes stream_end]
#   ./auto_command.sh dimension

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
LOG_FILE="$LOG_DIR/pipeline.log"
VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"
VENV_DBT="$SCRIPT_DIR/.venv/bin/dbt"

mkdir -p "$LOG_DIR"
cd "$SCRIPT_DIR"

# Export .env into this shell - dbt and the Python scripts both read env
# vars directly, not the .env file. Read line by line (not `source .env`)
# because AIRFLOW__SMTP__SMTP_PASSWORD contains unquoted spaces.
while IFS='=' read -r env_key env_value; do
    if [[ -z "$env_key" || "$env_key" == \#* ]]; then
        continue
    fi
    export "$env_key=$env_value"
done < <(grep -v '^[[:space:]]*#' "$SCRIPT_DIR/.env" | grep -v '^[[:space:]]*$')

log() {
    printf '%s %s\n' "$(date +'%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG_FILE"
}

run_step() {
    local name="$1"
    shift
    log "START $name"
    local start_sec
    start_sec=$(date +%s)
    if "$@" 2>&1 | tee -a "$LOG_FILE"; then
        local status=0
    else
        local status=$?
    fi
    local end_sec
    end_sec=$(date +%s)
    local duration=$((end_sec - start_sec))
    if [[ $status -ne 0 ]]; then
        log "ERROR $name exited with status=$status after ${duration}s"
        exit "$status"
    fi
    log "END   $name duration=${duration}s"
}

run_batch() {
    # Same params as fire_ingestion_dag.py. With no arguments this uses
    # whatever range is set in .env - pass explicit dates for a cheaper run.
    if [[ $# -ge 1 ]]; then
        export PETABENCANA_ARCHIVE_START="$1"
    fi
    if [[ $# -ge 2 ]]; then
        export PETABENCANA_ARCHIVE_END="$2"
    fi

    log "BATCH PIPELINE START (range: ${PETABENCANA_ARCHIVE_START} to ${PETABENCANA_ARCHIVE_END})"
    run_step "fetch_fire_reports_to_gcs" "$VENV_PYTHON" ingestion/fetch_batch_to_gcs.py
    run_step "load_raw_to_bigquery" "$VENV_PYTHON" batch/load_raw_to_bigquery.py
    run_step "dbt_staging" bash -c "cd '$SCRIPT_DIR/dbt' && '$VENV_DBT' run --select disaster_batch_staging"
    run_step "dbt_dq" bash -c "cd '$SCRIPT_DIR/dbt' && '$VENV_DBT' run --select disaster_batch_staging_dq"
    log "BATCH PIPELINE COMPLETE"
}

run_stream() {
    # Same params as fire_stream_ingestion_dag.py.
    if [[ $# -ge 1 ]]; then
        export STREAM_LOOKBACK_MINUTES="$1"
    fi
    if [[ $# -ge 2 ]]; then
        export STREAM_END="$2"
    fi

    log "STREAM PIPELINE START (lookback: ${STREAM_LOOKBACK_MINUTES:-30} minutes)"
    run_step "publish_fire_reports_to_pubsub" "$VENV_PYTHON" ingestion/publish_stream_to_pubsub.py
    run_step "consume_fire_reports_to_bigquery" "$VENV_PYTHON" stream/load_stream_to_bigquery.py
    run_step "dbt_stream_staging" bash -c "cd '$SCRIPT_DIR/dbt' && '$VENV_DBT' build --select disaster_stream_staging"
    run_step "dbt_stream_dq" bash -c "cd '$SCRIPT_DIR/dbt' && '$VENV_DBT' build --select disaster_stream_staging_dq"
    log "STREAM PIPELINE COMPLETE"
}

run_dimension() {
    # No overrides needed - one extract + one load, then exit.
    log "DIMENSION PIPELINE START"
    run_step "extract_dimension_to_postgres" "$VENV_PYTHON" dimension/extract_dimension_to_postgres.py
    run_step "load_dimension_to_bigquery" "$VENV_PYTHON" dimension/load_dimension_to_bigquery.py
    log "DIMENSION PIPELINE COMPLETE"
}

usage() {
    echo "Usage: $0 batch [start_date end_date] | stream [lookback_minutes stream_end] | dimension"
    echo ""
    echo "  batch [start_date end_date]"
    echo "      Same 4 steps as airflow/dags/fire_ingestion_dag.py:"
    echo "      fetch -> load_raw -> dbt_staging -> dbt_dq"
    echo "      Defaults to PETABENCANA_ARCHIVE_START/END from .env if omitted."
    echo ""
    echo "  stream [lookback_minutes stream_end]"
    echo "      Same 4 steps as airflow/dags/fire_stream_ingestion_dag.py:"
    echo "      publish -> consume -> dbt_stream_staging -> dbt_stream_dq"
    echo "      Defaults to STREAM_LOOKBACK_MINUTES from .env (30) if omitted."
    echo ""
    echo "  dimension"
    echo "      GitHub JSON -> PostgreSQL -> BigQuery for dim_region/dim_province:"
    echo "      extract_dimension_to_postgres -> load_dimension_to_bigquery"
    exit 1
}

case "${1:-}" in
    batch)
        shift
        run_batch "$@"
        ;;
    stream)
        shift
        run_stream "$@"
        ;;
    dimension)
        shift
        run_dimension "$@"
        ;;
    *)
        usage
        ;;
esac
