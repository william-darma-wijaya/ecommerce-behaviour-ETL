{{ config(materialized='table') }}
{# Invalid and duplicate category rows, as they arrived. bronze = slv_category_tree + quarantine. #}

SELECT
    categoryid, parentid,
    batch_id, delivered_at, ingested_at, source_file, source_row_number,
    reasons
FROM {{ ref('slv_category_tree_checked') }}
WHERE reasons IS NOT NULL
