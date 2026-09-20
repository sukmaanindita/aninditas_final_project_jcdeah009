{{ config(materialized='table') }}

-- Same rules and pattern as disaster_batch_staging_dq.sql, for disaster_stream_staging.

with staging as (

    select *
    from {{ ref('disaster_stream_staging') }}

),

dim_region as (

    select *
    from {{ source('dimension', 'dim_region') }}

),

-- Rule 1: required fields must not be NULL. pkey is excluded - it's allowed
-- to be NULL, record_key has a fallback formula for that case.
required_field_checks as (

    select
        s.record_key,
        s.pkey,
        'required_field' as check_name,
        field.column_name,
        'missing_required_field' as issue_type,
        concat(field.column_name, ' is NULL') as issue_detail,
        current_timestamp() as checked_at
    from staging as s,
    unnest([
        struct('record_key' as column_name, s.record_key is null as is_null),
        struct('created_at' as column_name, s.created_at is null as is_null),
        struct('disaster_type' as column_name, s.disaster_type is null as is_null),
        struct('fire_lat' as column_name, s.fire_lat is null as is_null),
        struct('fire_lng' as column_name, s.fire_lng is null as is_null),
        struct('person_lat' as column_name, s.person_lat is null as is_null),
        struct('person_lng' as column_name, s.person_lng is null as is_null),
        struct('url' as column_name, s.url is null as is_null)
    ]) as field
    where field.is_null

),

-- Rule 2a: region_code is NULL
region_code_null_checks as (

    select
        record_key,
        pkey,
        'region_code_check' as check_name,
        'region_code' as column_name,
        'region_code_null' as issue_type,
        'region_code is NULL' as issue_detail,
        current_timestamp() as checked_at
    from staging
    where region_code is null

),

-- Rule 2b: region_code is present but not found in dim_region.id
region_code_not_found_checks as (

    select
        s.record_key,
        s.pkey,
        'region_code_check' as check_name,
        'region_code' as column_name,
        'region_code_not_found_in_dimension' as issue_type,
        concat('region_code ', s.region_code, ' not found in dim_region.id') as issue_detail,
        current_timestamp() as checked_at
    from staging as s
    where s.region_code is not null
      and not exists (
        select 1 from dim_region as d where d.id = s.region_code
      )

),

-- Rule 3: staging.city vs dim_region.name mismatch for the same region_code.
-- Deterministic normalization (uppercase, strip KABUPATEN/KOTA, remove spaces) - no fuzzy matching.
normalized as (

    select
        s.record_key,
        s.pkey,
        s.city,
        s.region_code,
        d.name as dim_name,
        regexp_replace(regexp_replace(upper(s.city), r'^(KABUPATEN|KOTA)\s+', ''), r'\s+', '') as city_normalized,
        regexp_replace(regexp_replace(upper(d.name), r'^(KABUPATEN|KOTA)\s+', ''), r'\s+', '') as dim_name_normalized
    from staging as s
    inner join dim_region as d
        on s.region_code = d.id
    where s.city is not null

),

source_inconsistency_checks as (

    select
        record_key,
        pkey,
        'source_inconsistency' as check_name,
        'city' as column_name,
        'city_region_code_mismatch' as issue_type,
        concat('source city=', city, ' vs dim_region.name for region_code ', region_code, '=', dim_name) as issue_detail,
        current_timestamp() as checked_at
    from normalized
    where city_normalized != dim_name_normalized

),

-- Rule 4: duplicate record_key. Should not happen (staging is an
-- incremental model with unique_key=record_key), checked here defensively
-- rather than assumed.
duplicate_checks as (

    select
        record_key,
        pkey,
        'duplicate_check' as check_name,
        'record_key' as column_name,
        'duplicate_record_key' as issue_type,
        concat('record_key appears ', cast(key_count as string), ' times in disaster_stream_staging') as issue_detail,
        current_timestamp() as checked_at
    from (
        select record_key, pkey, count(*) over (partition by record_key) as key_count
        from staging
    )
    where key_count > 1

)

select * from required_field_checks
union all
select * from region_code_null_checks
union all
select * from region_code_not_found_checks
union all
select * from source_inconsistency_checks
union all
select * from duplicate_checks
