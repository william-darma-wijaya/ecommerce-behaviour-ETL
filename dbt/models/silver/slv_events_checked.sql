{{ config(materialized='ephemeral') }}
{# Bronze events of this batch, cleaned, checked and typed. reasons is NULL for a valid row. #}

WITH cleaned AS (
    SELECT
        {{ clean('timestamp') }}             AS ts,
        {{ clean('visitorid') }}             AS visitor,
        lower({{ clean('event') }})          AS ev,
        {{ clean('itemid') }}                AS item,
        {{ clean('transactionid') }}         AS tx,
        *
    FROM {{ source('bronze', 'brz_events') }}
    WHERE batch_id = {{ batch_id() }}
),

typed AS (
    SELECT *,
        CASE WHEN regexp_full_match(ts, '[0-9]{13}') THEN epoch_ms(ts::BIGINT) END AS event_time,
        row_number() OVER (PARTITION BY ts, visitor, ev, item, tx ORDER BY source_file, source_row_number) AS copy_no
    FROM cleaned
),

checked AS (
    SELECT *,
        nullif(concat_ws(';',
            CASE WHEN event_time IS NULL THEN 'INVALID_TIMESTAMP' END,
            CASE WHEN event_time > delivered_at THEN 'FUTURE_EVENT' END,
            CASE WHEN NOT {{ is_digits('visitor') }} THEN 'INVALID_VISITORID' END,
            CASE WHEN ev IS NULL OR ev NOT IN ('view', 'addtocart', 'transaction') THEN 'INVALID_EVENT_TYPE' END,
            CASE WHEN NOT {{ is_digits('item') }} THEN 'INVALID_ITEMID' END,
            CASE WHEN ev = 'transaction' AND NOT {{ is_digits('tx') }} THEN 'MISSING_TRANSACTIONID' END,
            CASE WHEN ev IN ('view', 'addtocart') AND tx IS NOT NULL THEN 'UNEXPECTED_TRANSACTIONID' END,
            CASE WHEN copy_no > 1 THEN 'DUPLICATE' END
        ), '') AS reasons
    FROM typed
)

SELECT
    -- original bronze columns (kept for the quarantine)
    timestamp, visitorid, event, itemid, transactionid,
    batch_id, delivered_at, ingested_at, source_file, source_row_number,
    reasons,
    -- typed values, only meaningful when reasons IS NULL
    CASE WHEN reasons IS NULL THEN md5(concat_ws('|', ts, visitor, ev, item, coalesce(tx, ''))) END AS event_key,
    try_cast(ts AS BIGINT)        AS event_ts_ms,
    event_time,
    try_cast(visitor AS BIGINT)   AS visitor_id,
    ev                            AS event_type,
    try_cast(item AS BIGINT)      AS item_id,
    try_cast(tx AS BIGINT)        AS transaction_id
FROM checked
