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

SELECT
    _source_month,
    pickup_date_key,
    pickup_hour,
    COUNT(*) AS trip_count,
    SUM(total_amount) AS recorded_total_amount,
    AVG(duration_seconds) / 60.0 AS avg_duration_minutes
FROM {{ ref('fact_taxi_trips') }}
WHERE 1 = 1
{% if is_incremental() %}
  AND _source_month = '{{ source_month }}'
{% endif %}
GROUP BY _source_month, pickup_date_key, pickup_hour
