{{ config(materialized='incremental', incremental_strategy='delete+insert',
          unique_key=['item_id', 'property', 'changed_at']) }}

SELECT
    item_id,
    prop        AS property,
    changed_at,
    changed_at_ms,
    value_raw,
    value_num,
    CASE WHEN value_num IS NULL THEN value_raw END AS value_text,
    batch_id, delivered_at, ingested_at, source_file, source_row_number
FROM {{ ref('slv_item_properties_checked') }}
WHERE reasons IS NULL
