{{ config(materialized='table', schema='gold') }}

WITH bounds AS (
    SELECT
        LEAST(MIN(CAST(tpep_pickup_datetime AS DATE)),
              MIN(CAST(tpep_dropoff_datetime AS DATE))) AS first_date,
        GREATEST(MAX(CAST(tpep_pickup_datetime AS DATE)),
                 MAX(CAST(tpep_dropoff_datetime AS DATE))) AS last_date
    FROM {{ ref('silver_yellow_taxi_trips') }}
    WHERE overall_trip_quality = 'VALID'
),
calendar AS (
    SELECT EXPLODE(SEQUENCE(first_date, last_date, INTERVAL 1 DAY)) AS full_date
    FROM bounds
    WHERE first_date IS NOT NULL AND last_date IS NOT NULL
)
SELECT
    CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    YEAR(full_date) AS year,
    QUARTER(full_date) AS quarter,
    MONTH(full_date) AS month,
    DATE_FORMAT(full_date, 'MMMM') AS month_name,
    DAY(full_date) AS day_of_month,
    DAYOFWEEK(full_date) AS day_of_week,
    DATE_FORMAT(full_date, 'EEEE') AS day_name,
    CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM calendar
