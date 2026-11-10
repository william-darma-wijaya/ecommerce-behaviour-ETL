{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='item_id') }}
{# SCD Type 2 on categoryid and available. Affected items get all their versions rebuilt from the full
   history, so late data is handled like new data. #}

WITH affected AS (
{% if is_incremental() %}
    -- items with new categoryid/available changes in this batch
    SELECT item_id FROM {{ ref('slv_item_property_history') }}
    WHERE batch_id = {{ batch_id() }} AND property IN ('categoryid', 'available')
    UNION
    -- items clicked in this batch that are not in dim_item yet (they get a placeholder)
    SELECT e.item_id FROM {{ ref('slv_events') }} e
    WHERE e.batch_id = {{ batch_id() }}
      AND NOT EXISTS (SELECT 1 FROM {{ this }} d WHERE d.item_id = e.item_id)
    UNION
    -- items pointing to Unknown whose category now exists in dim_category
    SELECT d.item_id FROM {{ this }} d
    JOIN {{ ref('dim_category') }} c ON c.categoryid = d.categoryid
    WHERE d.category_key = '-1'
{% else %}
    SELECT item_id FROM {{ ref('slv_item_property_history') }} WHERE property IN ('categoryid', 'available')
    UNION
    SELECT item_id FROM {{ ref('slv_events') }}
{% endif %}
),

props AS (
    -- one row per change moment, with the new categoryid and/or available (NULL = not changed then)
    SELECT h.item_id, h.changed_at,
           max(CASE WHEN h.property = 'categoryid' THEN h.value_raw END) AS categoryid_chg,
           max(CASE WHEN h.property = 'available'  THEN h.value_raw END) AS available_chg
    FROM {{ ref('slv_item_property_history') }} h
    JOIN affected a ON a.item_id = h.item_id
    WHERE h.property IN ('categoryid', 'available')
    GROUP BY h.item_id, h.changed_at
),

filled AS (
    -- carry the last known value forward when it did not change at that moment
    SELECT item_id, changed_at,
           last_value(categoryid_chg IGNORE NULLS) OVER w AS categoryid,
           last_value(available_chg  IGNORE NULLS) OVER w AS available_raw
    FROM props
    WINDOW w AS (PARTITION BY item_id ORDER BY changed_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

changes AS (
    -- keep only the moments where something really changed (later snapshots repeat unchanged values)
    SELECT * FROM filled
    QUALIFY categoryid    IS DISTINCT FROM lag(categoryid)    OVER (PARTITION BY item_id ORDER BY changed_at)
         OR available_raw IS DISTINCT FROM lag(available_raw) OVER (PARTITION BY item_id ORDER BY changed_at)
),

versions AS (
    -- each version is valid from its change until the next change; the first one from 1900-01-01
    SELECT item_id, categoryid,
           available_raw = '1' AS available,
           CASE WHEN row_number() OVER (PARTITION BY item_id ORDER BY changed_at) = 1
                THEN TIMESTAMP '1900-01-01' ELSE changed_at END AS valid_from,
           lead(changed_at, 1, TIMESTAMP '9999-12-31') OVER (PARTITION BY item_id ORDER BY changed_at) AS valid_to,
           false AS is_inferred
    FROM changes
),

placeholders AS (
    -- affected items with no categoryid/available data at all
    SELECT a.item_id,
           CAST(NULL AS VARCHAR)  AS categoryid,
           CAST(NULL AS BOOLEAN)  AS available,
           TIMESTAMP '1900-01-01' AS valid_from,
           TIMESTAMP '9999-12-31' AS valid_to,
           true AS is_inferred
    FROM affected a
    WHERE NOT EXISTS (SELECT 1 FROM props p WHERE p.item_id = a.item_id)
),

all_rows AS (
    SELECT * FROM versions
    UNION ALL
    SELECT * FROM placeholders
)

SELECT
    md5(CAST(r.item_id AS VARCHAR) || '|' || strftime(r.valid_from, '%Y-%m-%d %H:%M:%S')) AS item_key,
    r.item_id,
    r.categoryid,
    r.available,
    coalesce(c.category_key, '-1')                          AS category_key,
    r.valid_from,
    r.valid_to,
    r.is_inferred,
    (r.categoryid IS NOT NULL AND r.available IS NOT NULL)  AS is_complete,
    {{ batch_id() }}                                        AS updated_batch_id
FROM all_rows r
LEFT JOIN {{ ref('dim_category') }} c ON c.categoryid = r.categoryid
