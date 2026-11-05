
-- Combine individual quality flags into an overall trip classification.
-- Incremental strategy: replace one complete source month at a time.

{% set source_month = var('source_month', none) %}

{{ config(
    materialized='incremental',
    file_format='delta',
    incremental_strategy='replace_where',
    incremental_predicates="_source_month = '" ~ (source_month or '__initial__') ~ "'",
    on_schema_change='fail'
) }}

-- Require a source month for subsequent incremental runs.
{% if is_incremental() %}

    {% if not source_month %}
        {{ exceptions.raise_compiler_error(
            "source_month is required for incremental runs"
        ) }}
    {% endif %}

    {% if source_month | length != 7
       or source_month[4] != '-'
       or source_month[0:4] | int == 0
       or source_month[5:7] not in [
           '01', '02', '03', '04', '05', '06',
           '07', '08', '09', '10', '11', '12'
       ] %}
        {{ exceptions.raise_compiler_error(
            "Invalid source_month. Expected YYYY-MM."
        ) }}
    {% endif %}

{% endif %}

WITH flagged_trips AS (

    SELECT *
    FROM {{ ref('stg_yellow_taxi_trips') }}

    -- Only process the selected month on incremental runs.
    {% if is_incremental() %}
        WHERE _source_month = '{{ source_month }}'
    {% endif %}

),

classified_trips AS (

    SELECT
        *,

        CASE
            -- 1. Invalid measurements or timestamps
            WHEN passenger_count_quality = 'INVALID'
              OR distance_quality = 'INVALID'
              OR duration_quality IN ('INVALID', 'MISSING')
              OR timestamp_quality IN (
                  'MISSING',
                  'HISTORICAL_ANOMALY',
                  'FUTURE_ANOMALY'
              )
                THEN 'INVALID'

            -- 2. Multiple severe anomalies
            WHEN distance_quality = 'ZERO'
             AND duration_quality = 'ZERO'
             AND (
                 passenger_count_quality = 'ZERO'
                 OR amount_quality = 'NEGATIVE'
             )
                THEN 'HIGHLY_SUSPICIOUS'

            -- 3. Related anomaly combinations
            WHEN (
                distance_quality = 'ZERO'
                AND passenger_count_quality = 'ZERO'
            )
            OR (
                distance_quality = 'ZERO'
                AND duration_quality = 'ZERO'
            )
            OR (
                duration_quality = 'ZERO'
                AND passenger_count_quality = 'ZERO'
            )
                THEN 'SUSPICIOUS'

            -- 4. Unusual measurements requiring review
            WHEN passenger_count_quality IN ('ZERO', 'REVIEW')
              OR distance_quality = 'ZERO'
              OR amount_quality IN ('NEGATIVE', 'ZERO')
              OR duration_quality IN (
                  'ZERO',
                  'REVIEW',
                  'EXTREME',
                  'HIGHLY_EXTREME'
              )
              OR timestamp_quality = 'BOUNDARY_REVIEW'
                THEN 'REVIEW'

            -- 5. Missing nonessential measurements
            WHEN passenger_count_quality = 'MISSING'
              OR distance_quality = 'MISSING'
              OR amount_quality = 'MISSING'
                THEN 'INCOMPLETE'

            -- 6. All quality dimensions acceptable
            WHEN passenger_count_quality = 'VALID'
             AND distance_quality = 'VALID'
             AND amount_quality = 'POSITIVE'
             AND duration_quality = 'VALID'
             AND timestamp_quality = 'VALID'
                THEN 'VALID'

            ELSE 'REVIEW'

        END AS overall_trip_quality

    FROM flagged_trips

)

SELECT *
FROM classified_trips
