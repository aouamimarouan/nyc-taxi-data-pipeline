-- All six classifications in aggregate must reconcile with Silver total.
WITH silver_months AS (
    SELECT _source_month, COUNT(*) AS n
    FROM {{ ref('silver_yellow_taxi_trips') }}
    GROUP BY _source_month
),
agg_months AS (
    SELECT _source_month, SUM(trip_count) AS n
    FROM {{ ref('agg_data_quality') }}
    GROUP BY _source_month
)
SELECT COALESCE(s._source_month, a._source_month) AS source_month,
       s.n AS silver_rows, a.n AS agg_rows
FROM silver_months s
FULL OUTER JOIN agg_months a ON s._source_month = a._source_month
WHERE s.n IS NULL OR a.n IS NULL OR s.n <> a.n
