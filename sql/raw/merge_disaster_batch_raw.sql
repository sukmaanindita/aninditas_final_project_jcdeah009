-- Merge one date's worth of reports (loaded into a temporary table) into the
-- raw table, keyed by record_key. This is what makes re-running the same
-- date safe: matching rows get updated in place, new rows get inserted, and
-- no duplicate rows are ever created.
--
-- {{ target_table }} and {{ temp_table }} are replaced by the Python script
-- before this query is run (simple string replace, no templating library).

MERGE `{{ target_table }}` AS target
USING `{{ temp_table }}` AS source
ON target.record_key = source.record_key

WHEN MATCHED THEN
  UPDATE SET
    pkey = source.pkey,
    created_at = source.created_at,
    disaster_type = source.disaster_type,
    raw_geometry = source.raw_geometry,
    source_file = source.source_file,
    ingested_at = source.ingested_at

WHEN NOT MATCHED THEN
  INSERT (record_key, pkey, created_at, disaster_type, raw_geometry, source_file, ingested_at)
  VALUES (source.record_key, source.pkey, source.created_at, source.disaster_type, source.raw_geometry, source.source_file, source.ingested_at);
