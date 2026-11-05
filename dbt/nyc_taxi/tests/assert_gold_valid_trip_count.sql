-- All Gold source-month row counts must match VALID Silver source-month counts.
WITH silver_months AS (
    SELECT _source_month, COUNT(*) AS n
    FROM {{ ref('silver_yellow_taxi_trips') }}
    WHERE overall_trip_quality = 'VALID'
    GROUP BY _source_month
),
gold_months AS (
    SELECT _source_month, COUNT(*) AS n
    FROM {{ ref('fact_taxi_trips') }}
    GROUP BY _source_month
)
SELECT COALESCE(s._source_month, g._source_month) AS source_month,
       s.n AS silver_valid_rows, g.n AS gold_fact_rows
FROM silver_months s
FULL OUTER JOIN gold_months g ON s._source_month = g._source_month
WHERE s.n IS NULL OR g.n IS NULL OR s.n <> g.n
