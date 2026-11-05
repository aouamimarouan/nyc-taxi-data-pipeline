
{% macro mark_silver_processed(source_month) %}

    {# 1. Validate the source month #}
    {% if not modules.re.match('^[0-9]{4}-(0[1-9]|1[0-2])$', source_month | string) %}
        {{ exceptions.raise_compiler_error(
            "Invalid source_month: " ~ source_month ~ ". Expected YYYY-MM."
        ) }}
    {% endif %}

    {# 2. Compare Bronze and Silver for the selected month #}
    {% set validation_sql %}

        WITH bronze AS (
            SELECT
                COUNT(*) AS row_count,
                COUNT(_source_version) AS non_null_versions,
                COUNT(DISTINCT _source_version) AS version_count,
                MAX(_source_version) AS source_version
            FROM nyc_project.bronze.yellow_taxi_trips
            WHERE _source_month = '{{ source_month }}'
        ),

        silver AS (
            SELECT
                COUNT(*) AS row_count,
                COUNT(_source_version) AS non_null_versions,
                COUNT(DISTINCT _source_version) AS version_count,
                MAX(_source_version) AS source_version
            FROM nyc_project.silver.silver_yellow_taxi_trips
            WHERE _source_month = '{{ source_month }}'
        )

        SELECT
            b.row_count AS bronze_rows,
            s.row_count AS silver_rows,
            b.version_count AS bronze_version_count,
            s.version_count AS silver_version_count,
            b.source_version AS bronze_version,
            s.source_version AS silver_version,
            b.non_null_versions AS bronze_non_null_versions,
            s.non_null_versions AS silver_non_null_versions
        FROM bronze b
        CROSS JOIN silver s

    {% endset %}

    {% set result = run_query(validation_sql) %}

    {% if execute %}

        {% if result is none or result.rows | length != 1 %}
            {{ exceptions.raise_compiler_error(
                "Silver checkpoint validation query failed."
            ) }}
        {% endif %}

        {% set row = result.rows[0] %}

        {% set bronze_rows = row[0] | int %}
        {% set silver_rows = row[1] | int %}
        {% set bronze_version_count = row[2] | int %}
        {% set silver_version_count = row[3] | int %}
        {% set bronze_version = row[4] %}
        {% set silver_version = row[5] %}
        {% set bronze_non_null_versions = row[6] | int %}
        {% set silver_non_null_versions = row[7] | int %}

        {# 3. Fail without checkpointing if validation fails #}
        {% if bronze_rows == 0
            or bronze_rows != silver_rows
            or bronze_version_count != 1
            or silver_version_count != 1
            or bronze_non_null_versions != bronze_rows
            or silver_non_null_versions != silver_rows
            or bronze_version != silver_version %}

            {{ exceptions.raise_compiler_error(
                "Silver validation FAILED for " ~ source_month
                ~ " | Bronze rows: " ~ bronze_rows
                ~ " | Silver rows: " ~ silver_rows
                ~ " | Bronze version: " ~ bronze_version
                ~ " | Silver version: " ~ silver_version
            ) }}

        {% endif %}

        {# 4. Record successful Silver processing #}
        {% set checkpoint_sql %}

            MERGE INTO nyc_project.silver.silver_processing_control AS target

            USING (
                SELECT
                    '{{ source_month }}' AS source_month,
                    MAX(_source_version) AS source_version,
                    COUNT(*) AS row_count
                FROM nyc_project.silver.silver_yellow_taxi_trips
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

        {% do log(
            "Silver checkpoint SUCCESS | Month: " ~ source_month
            ~ " | Rows: " ~ silver_rows
            ~ " | Version: " ~ silver_version,
            info=True
        ) %}

    {% endif %}

{% endmacro %}
