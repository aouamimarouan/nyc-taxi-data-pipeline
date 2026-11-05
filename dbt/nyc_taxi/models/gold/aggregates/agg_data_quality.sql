{% set source_month = var('source_month', none) %}
{% if is_incremental() and (source_month is none or not modules.re.match('^[0-9]{4}-(0[1-9]|1[0-2])$', source_month | string)) %}
    {{ exceptions.raise_compiler_error('Incremental Gold aggregation requires --vars "source_month: YYYY-MM"') }}
{% endif %}
{{ config(
    materialized='incremental',
    schema='gold',
    file_format='delta',
    incremental_strategy='replace_where',
    incremental_predicates="_source_month = '" ~ (source_month or '__initial__') ~ "'",
    partition_by=['_source_month'],
    on_schema_change='fail'
) }}

-- Monitors ALL Silver records, including excluded INVALID/REVIEW/INCOMPLETE trips.
SELECT
    _source_month,
    overall_trip_quality,
    COUNT(*) AS trip_count,
    COUNT(DISTINCT _source_version) AS source_version_count,
    MAX(_source_version) AS source_version
FROM {{ ref('silver_yellow_taxi_trips') }}
WHERE 1 = 1
{% if is_incremental() %}
  AND _source_month = '{{ source_month }}'
{% endif %}
GROUP BY _source_month, overall_trip_quality
