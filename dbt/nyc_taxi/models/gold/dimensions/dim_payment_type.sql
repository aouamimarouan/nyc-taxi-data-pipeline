{{ config(materialized='table', schema='gold') }}

WITH known AS (
    SELECT * FROM VALUES
        (-1, 'Missing / unknown key'),
        ( 0, 'Flex Fare trip'),
        ( 1, 'Credit card'),
        ( 2, 'Cash'),
        ( 3, 'No charge'),
        ( 4, 'Dispute'),
        ( 5, 'Unknown'),
        ( 6, 'Voided trip')
    AS t(payment_type_key, payment_description)
),
observed AS (
    SELECT DISTINCT CAST(payment_type AS INT) AS payment_type_key
    FROM {{ ref('silver_yellow_taxi_trips') }}
    WHERE overall_trip_quality = 'VALID'
),
all_keys AS (
    SELECT payment_type_key FROM known
    UNION
    SELECT payment_type_key FROM observed WHERE payment_type_key IS NOT NULL
)
SELECT a.payment_type_key,
       COALESCE(k.payment_description, 'Unmapped code') AS payment_description
FROM all_keys a LEFT JOIN known k USING (payment_type_key)
