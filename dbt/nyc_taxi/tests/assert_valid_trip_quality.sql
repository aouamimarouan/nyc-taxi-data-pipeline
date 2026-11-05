-- Test that every VALID trip has valid individual flags

SELECT *
FROM {{ ref('silver_yellow_taxi_trips') }}

WHERE overall_trip_quality = 'VALID'
  AND (
       passenger_count_quality <> 'VALID'
    OR distance_quality <> 'VALID'
    OR amount_quality <> 'POSITIVE'
    OR duration_quality <> 'VALID'
    OR timestamp_quality <> 'VALID'
  )