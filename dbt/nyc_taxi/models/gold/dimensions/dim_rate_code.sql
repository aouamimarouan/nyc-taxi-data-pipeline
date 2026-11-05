{{ config(materialized='table', schema='gold') }}

WITH known AS (
    SELECT * FROM VALUES
        (-1, 'Missing / unknown key'),
        ( 1, 'Standard rate'),
        ( 2, 'JFK'),
        ( 3, 'Newark'),
        ( 4, 'Nassau or Westchester'),
        ( 5, 'Negotiated fare'),
        ( 6, 'Group ride'),
        (99, 'Null / unknown')
    AS t(rate_code_key, rate_description)
),
observed AS (
    SELECT DISTINCT CAST(RatecodeID AS INT) AS rate_code_key
    FROM {{ ref('silver_yellow_taxi_trips') }}
    WHERE overall_trip_quality = 'VALID'
),
all_keys AS (
    SELECT rate_code_key FROM known
    UNION
    SELECT rate_code_key FROM observed WHERE rate_code_key IS NOT NULL
)
SELECT a.rate_code_key,
       COALESCE(k.rate_description, 'Unmapped code') AS rate_description
FROM all_keys a LEFT JOIN known k USING (rate_code_key)
