{{ config(materialized='ephemeral') }}
{# All bronze category rows, cleaned and checked. reasons is NULL for a valid row. #}

WITH cleaned AS (
    SELECT
        {{ clean('categoryid') }}   AS cat,
        {{ clean('parentid') }}     AS parent,
        *
    FROM {{ source('bronze', 'brz_category_tree') }}
),

numbered AS (
    SELECT *,
        -- the tree has no timestamp, so we assume each file lists the newest rows first
        row_number() OVER (PARTITION BY cat ORDER BY ingested_at DESC, source_file, source_row_number) AS key_copy_no,
        row_number() OVER (PARTITION BY cat, parent ORDER BY ingested_at DESC, source_file, source_row_number) AS copy_no
    FROM cleaned
),

checked AS (
    SELECT *,
        nullif(concat_ws(';',
            CASE WHEN NOT {{ is_digits('cat') }} THEN 'INVALID_CATEGORYID' END,
            CASE WHEN parent IS NOT NULL AND NOT {{ is_digits('parent') }} THEN 'INVALID_PARENTID' END,
            CASE WHEN parent = cat THEN 'SELF_PARENT' END,
            CASE WHEN copy_no > 1 THEN 'DUPLICATE' END,
            CASE WHEN copy_no = 1 AND key_copy_no > 1 THEN 'CONFLICTING_VALUE' END
        ), '') AS reasons
    FROM numbered
)

SELECT
    categoryid, parentid,
    batch_id, delivered_at, ingested_at, source_file, source_row_number,
    reasons,
    cat, parent
FROM checked
