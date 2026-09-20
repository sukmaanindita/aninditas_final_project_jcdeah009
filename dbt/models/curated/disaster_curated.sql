{{ config(materialized='view') }}

-- Unified CURATED model: batch + streaming staging converged into one
-- dashboard-ready table (CLAUDE.md section 8).
with unioned_staging as (

    select * from {{ ref('disaster_batch_staging') }}
    union all
    select * from {{ ref('disaster_stream_staging') }}

),

-- record_key is source-agnostic, so the same report could in principle land in
-- both batch and stream - keep the most recently staged row per record_key.
deduplicated as (

    select *
    from unioned_staging
    qualify row_number() over (
        partition by record_key
        order by staged_at desc
    ) = 1

)

-- LEFT JOIN because region_code can be NULL or unmatched - those rows must
-- still show up in curated, just without a region/province name.
-- dim_region/dim_province are sources, not ref() - they're loaded by
-- dimension/load_dimension_to_bigquery.py from PostgreSQL, not dbt seed.
select
    d.record_key,
    d.created_at,
    d.staged_at,
    d.disaster_type,
    d.data_source,
    d.region_code,
    dim_region.province_id,
    dim_region.name as region_name,
    dim_province.name as province_name,
    d.fire_lat,
    d.fire_lng,
    d.city,
    d.text,
    d.status,
    d.image_url

from deduplicated as d
left join {{ source('dimension', 'dim_region') }} as dim_region
    on d.region_code = dim_region.id
left join {{ source('dimension', 'dim_province') }} as dim_province
    on dim_region.province_id = dim_province.id

-- Exclude simulation/drill reports from the dashboard - raw and staging keep them for audit.
where lower(coalesce(d.text, '')) not like '%simulasi%'
