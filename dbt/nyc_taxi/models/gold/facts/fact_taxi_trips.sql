{% set source_month = var('source_month', none) %}

{% if is_incremental() and (source_month is none or not modules.re.match('^[0-9]{4}-(0[1-9]|1[0-2])$', source_month | string)) %}
    {{ exceptions.raise_compiler_error('Incremental Gold fact requires --vars "source_month: YYYY-MM"') }}
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

-- Grain: one source trip row classified VALID. No synthetic unique trip key.
-- Never DISTINCT these records: two legitimate trips may share identical values.
SELECT
    CAST(DATE_FORMAT(tpep_pickup_datetime, 'yyyyMMdd') AS INT) AS pickup_date_key,
    CAST(DATE_FORMAT(tpep_dropoff_datetime, 'yyyyMMdd') AS INT) AS dropoff_date_key,
    COALESCE(CAST(PULocationID AS INT), -1) AS pickup_location_key,
    COALESCE(CAST(DOLocationID AS INT), -1) AS dropoff_location_key,
    COALESCE(CAST(payment_type AS INT), -1) AS payment_type_key,
    COALESCE(CAST(VendorID AS INT), -1) AS vendor_key,
    COALESCE(CAST(RatecodeID AS INT), -1) AS rate_code_key,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    CAST(HOUR(tpep_pickup_datetime) AS INT) AS pickup_hour,
    passenger_count,
    trip_distance AS trip_distance_miles,
    duration_seconds,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee AS airport_fee,
    cbd_congestion_fee,
    store_and_fwd_flag,
    request_source,
    _source_month,
    _source_version,
    _source_file,
    _bronze_ingested_at
FROM {{ ref('silver_yellow_taxi_trips') }}
WHERE overall_trip_quality = 'VALID'
{% if is_incremental() %}
  AND _source_month = '{{ source_month }}'
{% endif %}
