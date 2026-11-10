{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='event_key') }}
{# One row per event, pointing to the item version valid at the event time. Events of items whose
   versions were rewritten in this batch are written again, since a late property can move them. #}

WITH to_write AS (
    SELECT * FROM {{ ref('slv_events') }}
{% if is_incremental() %}
    WHERE batch_id = {{ batch_id() }}
       OR item_id IN (SELECT item_id FROM {{ ref('dim_item') }} WHERE updated_batch_id = {{ batch_id() }})
{% endif %}
)

SELECT
    e.event_key,
    e.event_time,
    CAST(strftime(e.event_time, '%Y%m%d') AS INTEGER)  AS date_key,
    t.event_type_key,
    d.item_key,
    e.visitor_id,
    e.transaction_id,
    CAST(1 AS SMALLINT)                                 AS event_count,
    e.batch_id                                          AS arrival_batch_id,
    {{ batch_id() }}                                    AS updated_batch_id
FROM to_write e
JOIN {{ ref('dim_item') }} d
  ON  d.item_id = e.item_id
  AND e.event_time >= d.valid_from
  AND e.event_time <  d.valid_to
JOIN {{ ref('dim_event_type') }} t ON t.event_type = e.event_type
