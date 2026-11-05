-- Test that Silver preserves the Bronze record count

WITH bronze AS (
    SELECT COUNT(*) AS total
    FROM {{ source('bronze', 'yellow_taxi_trips') }}
),

silver AS (
    SELECT COUNT(*) AS total
    FROM {{ ref('silver_yellow_taxi_trips') }}
)

SELECT
    bronze.total AS bronze_count,
    silver.total AS silver_count
FROM bronze
CROSS JOIN silver
WHERE bronze.total <> silver.total