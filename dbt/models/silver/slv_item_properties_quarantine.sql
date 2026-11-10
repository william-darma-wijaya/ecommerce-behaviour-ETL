{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='batch_id') }}
{# Invalid and duplicate property rows, as they arrived. Per batch: bronze = history + quarantine. #}

SELECT
    timestamp, itemid, property, value,
    batch_id, delivered_at, ingested_at, source_file, source_row_number,
    reasons
FROM {{ ref('slv_item_properties_checked') }}
WHERE reasons IS NOT NULL
