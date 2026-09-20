{{
  config(
    materialized='incremental',
    unique_key='record_key'
  )
}}

-- disaster_stream_raw is already pre-filtered to fire at publish time, so this
-- WHERE is defensive, kept for consistency with disaster_batch_staging.sql.
with raw_fire_reports as (

    select *
    from {{ source('raw', 'disaster_stream_raw') }}
    where disaster_type = 'fire'

)

-- Identical parsing logic to disaster_batch_staging.sql - raw_geometry has the same shape.
select
    record_key,
    pkey,
    created_at,
    date(created_at) as report_date,
    disaster_type,
    json_extract_scalar(raw_geometry, '$.properties.report_data.report_type') as report_type,
    cast(json_extract_scalar(raw_geometry, '$.properties.report_data.fireDistance') as float64) as fire_distance,
    cast(json_extract_scalar(raw_geometry, '$.properties.report_data.fireLocation.lat') as float64) as fire_lat,
    cast(json_extract_scalar(raw_geometry, '$.properties.report_data.fireLocation.lng') as float64) as fire_lng,
    cast(json_extract_scalar(raw_geometry, '$.properties.report_data.personLocation.lat') as float64) as person_lat,
    cast(json_extract_scalar(raw_geometry, '$.properties.report_data.personLocation.lng') as float64) as person_lng,
    cast(json_extract_scalar(raw_geometry, '$.properties.report_data.fireRadius.lat') as float64) as fire_radius_lat,
    cast(json_extract_scalar(raw_geometry, '$.properties.report_data.fireRadius.lng') as float64) as fire_radius_lng,
    json_extract_scalar(raw_geometry, '$.properties.tags.city') as city,
    json_extract_scalar(raw_geometry, '$.properties.tags.region_code') as region_code,
    json_extract_scalar(raw_geometry, '$.properties.tags.instance_region_code') as instance_region_code,
    json_extract_scalar(raw_geometry, '$.properties.tags.district_id') as district_id,
    json_extract_scalar(raw_geometry, '$.properties.tags.local_area_id') as local_area_id,
    json_extract_scalar(raw_geometry, '$.properties.source') as source,
    json_extract_scalar(raw_geometry, '$.properties.status') as status,
    json_extract_scalar(raw_geometry, '$.properties.title') as title,
    json_extract_scalar(raw_geometry, '$.properties.text') as text,
    json_extract_scalar(raw_geometry, '$.properties.url') as url,
    json_extract_scalar(raw_geometry, '$.properties.image_url') as image_url,
    raw_geometry,
    source_file,
    'petabencana_stream' as data_source,
    ingested_at,
    current_timestamp() as staged_at,
    cast(null as string) as dq_status  -- DQ check is a separate, not-yet-built phase

from raw_fire_reports
