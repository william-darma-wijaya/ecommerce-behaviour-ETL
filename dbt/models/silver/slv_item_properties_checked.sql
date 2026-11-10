{{ config(materialized='ephemeral') }}
{# Bronze property rows of this batch, cleaned, checked and typed. reasons is NULL for a valid row. #}

WITH cleaned AS (
    SELECT
        {{ clean('timestamp') }}   AS ts,
        {{ clean('itemid') }}      AS item,
        {{ clean('property') }}    AS prop,
        {{ clean('value') }}       AS val,
        *
    FROM {{ source('bronze', 'brz_item_properties') }}
    WHERE batch_id = {{ batch_id() }}
),

numbered AS (
    SELECT *,
        row_number() OVER (PARTITION BY ts, item, prop ORDER BY source_file, source_row_number) AS key_copy_no,
        row_number() OVER (PARTITION BY ts, item, prop, val ORDER BY source_file, source_row_number) AS copy_no
    FROM cleaned
),

checked AS (
    SELECT *,
        nullif(concat_ws(';',
            CASE WHEN NOT coalesce(regexp_full_match(ts, '[0-9]{13}'), false) THEN 'INVALID_TIMESTAMP' END,
            CASE WHEN NOT {{ is_digits('item') }} THEN 'INVALID_ITEMID' END,
            CASE WHEN prop IS NULL THEN 'INVALID_PROPERTY' END,
            CASE WHEN val IS NULL THEN 'INVALID_VALUE' END,
            CASE WHEN prop = 'categoryid' AND val IS NOT NULL AND NOT {{ is_digits('val') }} THEN 'INVALID_CATEGORYID' END,
            CASE WHEN prop = 'available' AND val NOT IN ('0', '1') THEN 'INVALID_AVAILABLE' END,
            CASE WHEN copy_no > 1 THEN 'DUPLICATE' END,
            CASE WHEN copy_no = 1 AND key_copy_no > 1 THEN 'CONFLICTING_VALUE' END
        ), '') AS reasons
    FROM numbered
)
    
SELECT
    -- original bronze columns (kept for the quarantine)
    timestamp, itemid, property, value,
    batch_id, delivered_at, ingested_at, source_file, source_row_number,
    reasons,
    -- typed values, only meaningful when reasons IS NULL
    try_cast(item AS BIGINT)                                   AS item_id,
    prop,
    CASE WHEN reasons IS NULL THEN epoch_ms(ts::BIGINT) END    AS changed_at,
    try_cast(ts AS BIGINT)                                     AS changed_at_ms,
    val                                                        AS value_raw,
    -- numbers look like n15360.000; categoryid/available and multi-token values stay text
    CASE WHEN prop NOT IN ('categoryid', 'available') AND regexp_full_match(val, 'n-?[0-9]+(\.[0-9]+)?')
         THEN CAST(substr(val, 2) AS DOUBLE) END               AS value_num
FROM checked
