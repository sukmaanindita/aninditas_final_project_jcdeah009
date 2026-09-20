{#
  Override dbt's default schema-naming behavior for BigQuery.

  By default, dbt appends a custom schema to the profile's default schema
  (e.g. "fp_aninditas_staging_fp_aninditas_dimension"), which does not match
  this project's BigQuery layer datasets (raw / staging / curated /
  dimension - see CLAUDE.md). This macro makes a custom schema (set via
  +schema in dbt_project.yml) become the exact dataset name instead.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
