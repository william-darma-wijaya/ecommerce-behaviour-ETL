{{ config(materialized='view') }}

SELECT * FROM {{ ref('dim_item') }}
WHERE valid_to = TIMESTAMP '9999-12-31'
