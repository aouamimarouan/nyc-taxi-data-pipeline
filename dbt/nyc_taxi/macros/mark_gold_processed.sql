
{% macro mark_gold_processed(source_month) %}

    {# 1. Validate the source month #}
    {% if not modules.re.match(
        '^[0-9]{4}-(0[1-9]|1[0-2])$',
        source_month | string
    ) %}
        {{ exceptions.raise_compiler_error(
            "Invalid source_month: " ~ source_month
        ) }}
    {% endif %}

    {# 2. Validate Silver, Gold fact and all Gold aggregates #}
    {% set validation_sql %}

        WITH silver AS (
            SELECT
                COUNT(*) AS total_rows,
                COUNT_IF(overall_trip_quality = 'VALID') AS valid_rows,
                COUNT(DISTINCT _source_version) AS version_count,
                COUNT(_source_version) AS version_rows,
                MAX(_source_version) AS source_version
            FROM nyc_project.silver.silver_yellow_taxi_trips
            WHERE _source_month = '{{ source_month }}'
        ),

        silver_control AS (
            SELECT
                COUNT(*) AS successful_records,
                MAX(source_version) AS source_version
            FROM nyc_project.silver.silver_processing_control
            WHERE source_month = '{{ source_month }}'
              AND status = 'SUCCESS'
        ),

        gold_fact AS (
            SELECT
                COUNT(*) AS trip_count,
                COUNT(DISTINCT _source_version) AS version_count,
                COUNT(_source_version) AS version_rows,
                MAX(_source_version) AS source_version
            FROM nyc_project.gold.fact_taxi_trips
            WHERE _source_month = '{{ source_month }}'
        ),

        daily AS (
            SELECT
                COALESCE(SUM(trip_count), 0) AS trips
            FROM nyc_project.gold.agg_daily_trips
            WHERE _source_month = '{{ source_month }}'
        ),

        hourly AS (
            SELECT
                COALESCE(SUM(trip_count), 0) AS trips
            FROM nyc_project.gold.agg_hourly_demand
            WHERE _source_month = '{{ source_month }}'
        ),

        zones AS (
            SELECT
                COALESCE(SUM(pickup_trip_count), 0) AS trips
            FROM nyc_project.gold.agg_zone_performance
            WHERE _source_month = '{{ source_month }}'
        ),

        payments AS (
            SELECT
                COALESCE(SUM(trip_count), 0) AS trips
            FROM nyc_project.gold.agg_payment_analysis
            WHERE _source_month = '{{ source_month }}'
        ),

        quality AS (
            SELECT
                COALESCE(SUM(q.trip_count), 0) AS trips,

                COUNT(*) AS quality_groups,

                COUNT_IF(
                    q.source_version_count != 1
                    OR q.source_version_count IS NULL
                    OR q.source_version IS NULL
                ) AS invalid_version_groups,

                COUNT_IF(
                    NOT (q.source_version <=> s.source_version)
                ) AS mismatching_version_groups

            FROM nyc_project.gold.agg_data_quality AS q
            CROSS JOIN silver AS s

            WHERE q._source_month = '{{ source_month }}'
        )

        SELECT
          s.total_rows AS silver_total_rows,
          s.valid_rows AS silver_valid_rows,
          s.version_count AS silver_version_count,
          s.version_rows AS silver_version_rows,
          s.source_version AS silver_source_version,

          sc.successful_records AS silver_control_records,
          sc.source_version AS silver_control_source_version,

          f.trip_count AS gold_fact_rows,
          f.version_count AS gold_fact_version_count,
          f.version_rows AS gold_fact_version_rows,
          f.source_version AS gold_fact_source_version,

          d.trips AS daily_trips,
          h.trips AS hourly_trips,
          z.trips AS zone_trips,
          p.trips AS payment_trips,

          q.trips AS quality_trips,
          q.quality_groups AS quality_group_count,
          q.invalid_version_groups AS invalid_quality_versions,
          q.mismatching_version_groups AS mismatching_quality_versions

        FROM silver AS s
        CROSS JOIN silver_control AS sc
        CROSS JOIN gold_fact AS f
        CROSS JOIN daily AS d
        CROSS JOIN hourly AS h
        CROSS JOIN zones AS z
        CROSS JOIN payments AS p
        CROSS JOIN quality AS q

    {% endset %}

    {% if execute %}

        {% set result = run_query(validation_sql) %}

        {% if result is none or result.rows | length != 1 %}
            {{ exceptions.raise_compiler_error(
                "Gold validation query returned no valid result."
            ) }}
        {% endif %}

        {# 3. Extract validation results #}
        {% set r = result.rows[0] %}

        {% set silver_total = r[0] | int %}
        {% set silver_valid = r[1] | int %}
        {% set silver_versions = r[2] | int %}
        {% set silver_version_rows = r[3] | int %}
        {% set silver_version = r[4] %}

        {% set silver_control_count = r[5] | int %}
        {% set silver_control_version = r[6] %}

        {% set fact_count = r[7] | int %}
        {% set fact_versions = r[8] | int %}
        {% set fact_version_rows = r[9] | int %}
        {% set fact_version = r[10] %}

        {% set daily_count = r[11] | int %}
        {% set hourly_count = r[12] | int %}
        {% set zone_count = r[13] | int %}
        {% set payment_count = r[14] | int %}

        {% set quality_count = r[15] | int %}
        {% set quality_groups = r[16] | int %}
        {% set invalid_quality_versions = r[17] | int %}
        {% set mismatching_quality_groups = r[18] | int %}

        {# 4. Stop if any validation fails #}
        {% if silver_total == 0
            or silver_valid == 0

            or silver_versions != 1
            or silver_version_rows != silver_total

            or silver_control_count != 1
            or silver_control_version != silver_version

            or fact_count != silver_valid
            or fact_versions != 1
            or fact_version_rows != fact_count
            or fact_version != silver_version

            or daily_count != fact_count
            or hourly_count != fact_count
            or zone_count != fact_count
            or payment_count != fact_count

            or quality_count != silver_total
            or quality_groups == 0
            or invalid_quality_versions != 0
            or mismatching_quality_groups != 0 %}

            {{ exceptions.raise_compiler_error(
                "Gold validation FAILED for " ~ source_month
                ~ " | Silver total: " ~ silver_total
                ~ " | Silver VALID: " ~ silver_valid
                ~ " | Gold fact: " ~ fact_count
                ~ " | Daily: " ~ daily_count
                ~ " | Hourly: " ~ hourly_count
                ~ " | Zones: " ~ zone_count
                ~ " | Payments: " ~ payment_count
                ~ " | Quality: " ~ quality_count
                ~ " | Invalid quality versions: " ~ invalid_quality_versions
                ~ " | Mismatching quality versions: " ~ mismatching_quality_groups
            ) }}

        {% endif %}

        {# 5. Write the Gold checkpoint after validation #}
        {% set checkpoint_sql %}

            MERGE INTO nyc_project.gold.gold_processing_control AS target

            USING (
                SELECT
                    '{{ source_month }}' AS source_month,
                    MAX(_source_version) AS source_version,
                    COUNT(*) AS row_count
                FROM nyc_project.gold.fact_taxi_trips
                WHERE _source_month = '{{ source_month }}'
            ) AS source

            ON target.source_month = source.source_month

            WHEN MATCHED THEN UPDATE SET
                target.source_version = source.source_version,
                target.row_count = source.row_count,
                target.status = 'SUCCESS',
                target.processed_at = CURRENT_TIMESTAMP()

            WHEN NOT MATCHED THEN INSERT (
                source_month,
                source_version,
                row_count,
                status,
                processed_at
            )
            VALUES (
                source.source_month,
                source.source_version,
                source.row_count,
                'SUCCESS',
                CURRENT_TIMESTAMP()
            )

        {% endset %}

        {% do run_query(checkpoint_sql) %}

        {# 6. Log successful completion #}
        {% do log(
            "Gold checkpoint SUCCESS | Month: " ~ source_month
            ~ " | VALID trips: " ~ fact_count
            ~ " | Total Silver trips: " ~ silver_total
            ~ " | Version: " ~ silver_version,
            info=True
        ) %}

    {% endif %}

{% endmacro %}
