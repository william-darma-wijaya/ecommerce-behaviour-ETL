{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='batch_id') }}
{# Invalid and duplicate events, as they arrived. Per batch: bronze = slv_events + quarantine. #}

SELECT
    timestamp, visitorid, event, itemid, transactionid,
    batch_id, delivered_at, ingested_at, source_file, source_row_number,
    reasons
FROM {{ ref('slv_events_checked') }}
WHERE reasons IS NOT NULL
