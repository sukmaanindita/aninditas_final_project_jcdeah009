{{
  config(
    materialized='incremental',
    unique_key='record_key'
  )
}}

-- Other disaster_type values stay in RAW; filtering happens here, not upstream.
with raw_fire_reports as (

    select *
    from {{ source('raw', 'disaster_batch_raw') }}
    where disaster_type = 'fire'

),

-- Pull out city/region_code early so they can be matched against dim_region
-- below, before the rest of the fields are parsed in the final select.
parsed as (

    select
        *,
        json_extract_scalar(raw_geometry, '$.properties.tags.city') as city,
        json_extract_scalar(raw_geometry, '$.properties.tags.region_code') as region_code
    from raw_fire_reports

),

-- dim_region normalized for name matching (uppercase, strip KABUPATEN/KOTA,
-- remove spaces - same rule as the DQ source_inconsistency check), with a
-- count of how many regions share that normalized name. A handful of
-- Kabupaten/Kota pairs collide here (e.g. "BOGOR" = KABUPATEN BOGOR + KOTA
-- BOGOR, always in the same province) - name_normalized_count flags those.
dim_region_normalized as (

    select
        id,
        name,
        province_id,
        regexp_replace(regexp_replace(upper(name), r'^(KABUPATEN|KOTA)\s+', ''), r'\s+', '') as name_normalized,
        count(*) over (
            partition by regexp_replace(regexp_replace(upper(name), r'^(KABUPATEN|KOTA)\s+', ''), r'\s+', '')
        ) as name_normalized_count
    from {{ source('dimension', 'dim_region') }}

),

-- Region canonicalization: a name match (city vs dim_region.name) takes
-- priority over the source's own region_code - it wins even when
-- region_code disagrees or is NULL, as long as it uniquely identifies one
-- dim_region row. For the rare ambiguous Kabupaten/Kota pairs, the source
-- region_code is used only as a tie-breaker; if that doesn't resolve it
-- either, the name match is left unresolved rather than guessed.
name_matched as (

    select
        p.record_key,
        d.id as matched_id
    from parsed as p
    join dim_region_normalized as d
        on d.name_normalized = regexp_replace(regexp_replace(upper(coalesce(p.city, '')), r'^(KABUPATEN|KOTA)\s+', ''), r'\s+', '')
        and p.city is not null
        and (d.name_normalized_count = 1 or d.id = p.region_code)

),

-- Fallback: source region_code matches a real dim_region.id directly (used
-- when there is no usable city name to match by).
id_matched as (

    select
        p.record_key,
        d.id as matched_id
    from parsed as p
    join {{ source('dimension', 'dim_region') }} as d
        on d.id = p.region_code

)

-- Parse the fire-specific fields out of raw_geometry (JSON string); raw_geometry itself is kept too.
select
    parsed.record_key,
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
    city,
    -- Canonical region_code: name match wins, then a direct id match, then
    -- whatever the source gave (possibly invalid/NULL) - left for
    -- disaster_batch_staging_dq's existing region_code checks to flag.
    coalesce(name_matched.matched_id, id_matched.matched_id, region_code) as region_code,
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
    'petabencana_batch' as data_source,
    ingested_at,
    current_timestamp() as staged_at,
    cast(null as string) as dq_status  -- DQ check is a separate, not-yet-built phase

from parsed
left join name_matched on name_matched.record_key = parsed.record_key
left join id_matched on id_matched.record_key = parsed.record_key
