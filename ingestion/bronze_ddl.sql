-- Bronze keeps landing rows as text (nothing cast or rejected), plus load metadata.
-- delivered_at = simulated delivery time, ingested_at = real load time (UTC).
-- Key: (source_file, source_row_number), not declared as a constraint to keep loads fast.

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.brz_events (
    timestamp          VARCHAR,
    visitorid          VARCHAR,
    event              VARCHAR,
    itemid             VARCHAR,
    transactionid      VARCHAR,
    batch_id           VARCHAR   NOT NULL,
    delivered_at       TIMESTAMP NOT NULL,
    ingested_at        TIMESTAMP NOT NULL,
    source_file        VARCHAR   NOT NULL,
    source_row_number  INTEGER   NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.brz_item_properties (
    timestamp          VARCHAR,
    itemid             VARCHAR,
    property           VARCHAR,
    value              VARCHAR,
    batch_id           VARCHAR   NOT NULL,
    delivered_at       TIMESTAMP NOT NULL,
    ingested_at        TIMESTAMP NOT NULL,
    source_file        VARCHAR   NOT NULL,
    source_row_number  INTEGER   NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.brz_category_tree (
    categoryid         VARCHAR,
    parentid           VARCHAR,
    batch_id           VARCHAR   NOT NULL,
    delivered_at       TIMESTAMP NOT NULL,
    ingested_at        TIMESTAMP NOT NULL,
    source_file        VARCHAR   NOT NULL,
    source_row_number  INTEGER   NOT NULL
);
