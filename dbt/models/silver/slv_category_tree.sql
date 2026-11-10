{{ config(materialized='table') }}
{# One row per category, rebuilt every run. parentid NULL = root. #}

SELECT
    cat     AS categoryid,
    parent  AS parentid,
    batch_id, delivered_at, ingested_at, source_file, source_row_number
FROM {{ ref('slv_category_tree_checked') }}
WHERE reasons IS NULL
