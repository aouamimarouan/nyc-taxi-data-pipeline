{{ config(materialized='table', schema='gold') }}

-- The taxi_zone_lookup seed is the official NYC TLC CSV.
-- Include every observed source ID, even if the official lookup lacks a mapping.
WITH observed AS (
    SELECT CAST(PULocationID AS INT) AS location_key
    FROM {{ ref('silver_yellow_taxi_trips') }}
    WHERE overall_trip_quality = 'VALID'
    UNION
    SELECT CAST(DOLocationID AS INT) AS location_key
    FROM {{ ref('silver_yellow_taxi_trips') }}
    WHERE overall_trip_quality = 'VALID'
),
lookup AS (
    SELECT
        CAST(LocationID AS INT) AS location_key,
        Borough AS borough,
        Zone AS zone,
        service_zone
    FROM {{ ref('taxi_zone_lookup') }}
),
all_ids AS (
    SELECT location_key FROM observed WHERE location_key IS NOT NULL
    UNION
    SELECT location_key FROM lookup WHERE location_key IS NOT NULL
    UNION
    SELECT -1 AS location_key
)
SELECT
    ids.location_key,
    CASE WHEN ids.location_key = -1 THEN 'Unknown'
         ELSE COALESCE(l.borough, 'Unmapped') END AS borough,
    CASE WHEN ids.location_key = -1 THEN 'Unknown'
         ELSE COALESCE(l.zone, 'Unmapped') END AS zone,
    CASE WHEN ids.location_key = -1 THEN 'Unknown'
         ELSE COALESCE(l.service_zone, 'Unmapped') END AS service_zone
FROM all_ids ids
LEFT JOIN lookup l USING (location_key)
