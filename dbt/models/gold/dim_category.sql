{{ config(materialized='table') }}
{# Flattened category tree (root, level, path) plus the Unknown row '-1'. SCD Type 1, rebuilt every run. #}

WITH RECURSIVE tree AS (
    SELECT categoryid, parentid FROM {{ ref('slv_category_tree') }}
),

anchors AS (
    -- roots, or rows whose parent is missing from the tree
    SELECT t.categoryid, t.parentid FROM tree t
    WHERE t.parentid IS NULL
       OR NOT EXISTS (SELECT 1 FROM tree p WHERE p.categoryid = t.parentid)
),

walk AS (
    SELECT categoryid, parentid, categoryid AS root_categoryid, 0 AS level,
           categoryid AS path, (parentid IS NOT NULL) AS has_missing_parent
    FROM anchors
    UNION ALL
    SELECT c.categoryid, c.parentid, w.root_categoryid, w.level + 1,
           w.path || ' > ' || c.categoryid, w.has_missing_parent
    FROM tree c JOIN walk w ON c.parentid = w.categoryid
    WHERE w.level < 20    -- stops a cycle from looping forever
)

SELECT
    md5(w.categoryid)               AS category_key,
    w.categoryid,
    w.parentid,
    w.root_categoryid,
    CAST(w.level AS SMALLINT)       AS level,
    w.path,
    NOT EXISTS (SELECT 1 FROM tree x WHERE x.parentid = w.categoryid) AS is_leaf,
    w.has_missing_parent
FROM walk w

UNION ALL

SELECT '-1', NULL, NULL, NULL, NULL, 'Unknown', NULL, NULL
