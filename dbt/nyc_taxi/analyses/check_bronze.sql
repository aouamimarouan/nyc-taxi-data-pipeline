SELECT COUNT(*) AS total_trips
FROM {{ source('bronze', 'yellow_taxi_trips') }}