{{ config(materialized='table', schema='gold') }}

WITH known AS (
    SELECT * FROM VALUES
        (-1, 'Missing / unknown key'),
        ( 1, 'Creative Mobile Technologies, LLC'),
        ( 2, 'Curb Mobility, LLC'),
        ( 6, 'Myle Technologies Inc'),
        ( 7, 'Helix')
    AS t(vendor_key, vendor_name)
),
observed AS (
    SELECT DISTINCT CAST(VendorID AS INT) AS vendor_key
    FROM {{ ref('silver_yellow_taxi_trips') }}
    WHERE overall_trip_quality = 'VALID'
),
all_keys AS (
    SELECT vendor_key FROM known
    UNION
    SELECT vendor_key FROM observed WHERE vendor_key IS NOT NULL
)
SELECT a.vendor_key, COALESCE(k.vendor_name, 'Unmapped code') AS vendor_name
FROM all_keys a LEFT JOIN known k USING (vendor_key)
