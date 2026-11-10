{{ config(materialized='table') }}

WITH days AS (
    SELECT CAST(d AS DATE) AS date
    FROM range(DATE '{{ var("valid_event_time_range")[0] }}', DATE '{{ var("valid_event_time_range")[1] }}', INTERVAL 1 DAY) AS t(d)
)

SELECT
    CAST(strftime(date, '%Y%m%d') AS INTEGER)   AS date_key,
    date,
    CAST(year(date) AS SMALLINT)                AS year,
    CAST(quarter(date) AS SMALLINT)             AS quarter,
    CAST(month(date) AS SMALLINT)               AS month,
    monthname(date)                             AS month_name,
    CAST(weekofyear(date) AS SMALLINT)          AS iso_week,
    CAST(day(date) AS SMALLINT)                 AS day_of_month,
    CAST(isodow(date) AS SMALLINT)              AS iso_day_of_week,
    dayname(date)                               AS day_name,
    isodow(date) >= 6                           AS is_weekend
FROM days
