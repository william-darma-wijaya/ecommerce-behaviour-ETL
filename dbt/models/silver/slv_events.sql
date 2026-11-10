{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='event_key') }}

SELECT
    event_key, event_ts_ms, event_time,
    visitor_id, event_type, item_id, transaction_id,
    batch_id, delivered_at, ingested_at, source_file, source_row_number
FROM {{ ref('slv_events_checked') }}
WHERE reasons IS NULL
