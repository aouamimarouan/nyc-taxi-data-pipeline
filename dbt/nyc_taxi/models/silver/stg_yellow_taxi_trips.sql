
-- NYC Taxi Silver Staging
-- Calculate trip duration and individual data-quality flags.
-- Preserve all Bronze records and source lineage metadata.
-- Final Silver handles incremental month-based replacement.

{{ config(materialized='view') }}

-- Step 1: Read Bronze data
WITH source_data AS (

    SELECT *
    FROM {{ source('bronze', 'yellow_taxi_trips') }}

),

-- Step 2: Calculate duration
trip_preparation AS (

    SELECT
        *,
        TIMESTAMPDIFF(
            SECOND,
            tpep_pickup_datetime,
            tpep_dropoff_datetime
        ) AS duration_seconds

    FROM source_data

),

-- Step 3: Generate quality flags
quality_flags AS (

    SELECT
        *,

        -- Passenger count quality
        CASE
            WHEN passenger_count IS NULL THEN 'MISSING'
            WHEN passenger_count < 0 THEN 'INVALID'
            WHEN passenger_count = 0 THEN 'ZERO'
            WHEN passenger_count > 6 THEN 'REVIEW'
            ELSE 'VALID'
        END AS passenger_count_quality,

        -- Distance quality (miles)
        CASE
            WHEN trip_distance IS NULL THEN 'MISSING'
            WHEN trip_distance < 0 THEN 'INVALID'
            WHEN trip_distance = 0 THEN 'ZERO'
            ELSE 'VALID'
        END AS distance_quality,

        -- Amount quality
        CASE
            WHEN total_amount IS NULL THEN 'MISSING'
            WHEN total_amount < 0 THEN 'NEGATIVE'
            WHEN total_amount = 0 THEN 'ZERO'
            ELSE 'POSITIVE'
        END AS amount_quality,

        -- Duration quality (seconds)
        CASE
            WHEN duration_seconds IS NULL THEN 'MISSING'
            WHEN duration_seconds < 0 THEN 'INVALID'
            WHEN duration_seconds = 0 THEN 'ZERO'
            WHEN duration_seconds > 86400 THEN 'HIGHLY_EXTREME'
            WHEN duration_seconds > 21600 THEN 'EXTREME'
            WHEN duration_seconds > 7200 THEN 'REVIEW'
            ELSE 'VALID'
        END AS duration_quality,

        -- Timestamp quality
        -- No hardcoded upper reporting-month boundary
        CASE
            WHEN tpep_pickup_datetime IS NULL
              OR tpep_dropoff_datetime IS NULL
                THEN 'MISSING'

            WHEN tpep_pickup_datetime
                 < TIMESTAMP_NTZ '2025-12-01 00:00:00'
                THEN 'HISTORICAL_ANOMALY'

            WHEN tpep_pickup_datetime
                 < TIMESTAMP_NTZ '2026-01-01 00:00:00'
                THEN 'BOUNDARY_REVIEW'

            WHEN tpep_pickup_datetime >
                 CAST(
                     CURRENT_TIMESTAMP() + INTERVAL 1 DAY
                     AS TIMESTAMP_NTZ
                 )
                THEN 'FUTURE_ANOMALY'

            ELSE 'VALID'
        END AS timestamp_quality

    FROM trip_preparation

)

-- Step 4: Return all records and flags
SELECT *
FROM quality_flags
