{{ config(
    materialized='table',
    schema='consumption',
    alias='quote_intake_metrics_final'
) }}

WITH raw_metrics AS (
    SELECT
        service_name,
        timestamp,
        metric_key,
        metric_value
    FROM confirmed.quote_intake_metrics
    
    -- {{ source('confirmed', 'quote_intake_metrics') }}
)

SELECT
    service_name,
    timestamp,
    MAX(CASE WHEN metric_key = 'broker_name' THEN metric_value END) AS broker_name,
    MAX(CASE WHEN metric_key = 'employer_group_id' THEN metric_value END) AS employer_group_id,
    MAX(CASE WHEN metric_key = 'total_employees' THEN CAST(metric_value AS INT) END) AS total_employees,
    MAX(CASE WHEN metric_key = 'avg_age' THEN CAST(metric_value AS FLOAT) END) AS avg_age,
    MAX(CASE WHEN metric_key = 'plan_deductible' THEN CAST(metric_value AS FLOAT) END) AS plan_deductible
FROM raw_metrics
GROUP BY service_name, timestamp;