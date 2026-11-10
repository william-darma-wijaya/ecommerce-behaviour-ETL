"""ops.batch_log: one row per batch with its status and row counts.

The next batch is the oldest delivered one that is not `success`, so a failed batch is retried.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from simulator.clock import load_config

DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.batch_log (
    batch_id                      VARCHAR PRIMARY KEY,
    window_start                  TIMESTAMP,
    window_end                    TIMESTAMP,
    status                        VARCHAR,
    started_at                    TIMESTAMP,
    finished_at                   TIMESTAMP,
    attempt                       INTEGER,
    error_message                 VARCHAR,
    landing_rows_events           INTEGER,
    landing_rows_properties       INTEGER,
    landing_rows_category         INTEGER,
    bronze_rows_events            INTEGER,
    bronze_rows_properties        INTEGER,
    bronze_rows_category          INTEGER,
    silver_events_inserted        INTEGER,
    silver_events_quarantined     INTEGER,
    silver_properties_inserted    INTEGER,
    silver_properties_quarantined INTEGER,
    dim_item_items_rewritten      INTEGER,
    dim_item_placeholders_created INTEGER,
    fact_events_inserted          INTEGER,
    fact_events_reresolved        INTEGER,
    inferred_items_total          INTEGER,
    incomplete_items_total        INTEGER,
    unknown_category_items_total  INTEGER
);
"""


def now():
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)   # naive UTC, like bronze


def connect(cfg):
    return duckdb.connect(cfg["paths"]["warehouse_db"])


def setup(cfg):
    with connect(cfg) as con:
        con.execute(DDL).fetchall()


def manifests(cfg):
    """Delivered batches in processing order: INIT first, then ascending batch_id."""
    ids = [p.stem for p in (Path(cfg["paths"]["landing_dir"]) / "_manifests").glob("*.json")]
    return sorted(ids, key=lambda b: (b != "INIT", b))


def not_done(cfg):
    with connect(cfg) as con:
        done = {r[0] for r in con.execute("SELECT batch_id FROM ops.batch_log WHERE status = 'success'").fetchall()}
    return [b for b in manifests(cfg) if b not in done]


def start(cfg, batch_id):
    """Mark the batch running; a retry increments attempt."""
    m = json.loads((Path(cfg["paths"]["landing_dir"]) / "_manifests" / f"{batch_id}.json").read_text())
    rows = {f["source"]: f["rows"] for f in m["files"]}
    with connect(cfg) as con:
        con.execute("""
            INSERT INTO ops.batch_log (batch_id, window_start, window_end, status, started_at, attempt,
                                       landing_rows_events, landing_rows_properties, landing_rows_category)
            VALUES (?, ?, ?, 'running', ?, 1, ?, ?, ?)
            ON CONFLICT (batch_id) DO UPDATE SET
                status = 'running', started_at = excluded.started_at, finished_at = NULL,
                attempt = batch_log.attempt + 1, error_message = NULL
        """, [batch_id, m["window_start"], m["window_end"], now(),
              rows["events"], rows["item_properties"], rows["category_tree"]]).fetchall()


def pick_next_batch(cfg):
    """Start the oldest batch that is not `success`; None when there is nothing to do."""
    todo = not_done(cfg)
    if not todo:
        return None
    start(cfg, todo[0])
    return todo[0]


def finalize(cfg, batch_id):
    with connect(cfg) as con:
        con.execute("""
            UPDATE ops.batch_log SET
                bronze_rows_events            = (SELECT count(*) FROM bronze.brz_events                    WHERE batch_id = $b),
                bronze_rows_properties        = (SELECT count(*) FROM bronze.brz_item_properties           WHERE batch_id = $b),
                bronze_rows_category          = (SELECT count(*) FROM bronze.brz_category_tree             WHERE batch_id = $b),
                silver_events_inserted        = (SELECT count(*) FROM silver.slv_events                    WHERE batch_id = $b),
                silver_events_quarantined     = (SELECT count(*) FROM silver.slv_events_quarantine         WHERE batch_id = $b),
                silver_properties_inserted    = (SELECT count(*) FROM silver.slv_item_property_history     WHERE batch_id = $b),
                silver_properties_quarantined = (SELECT count(*) FROM silver.slv_item_properties_quarantine WHERE batch_id = $b),
                dim_item_items_rewritten      = (SELECT count(DISTINCT item_id) FROM gold.dim_item WHERE updated_batch_id = $b),
                dim_item_placeholders_created = (SELECT count(*) FROM gold.dim_item WHERE updated_batch_id = $b AND is_inferred),
                fact_events_inserted          = (SELECT count(*) FROM gold.fact_event WHERE arrival_batch_id = $b),
                fact_events_reresolved        = (SELECT count(*) FROM gold.fact_event WHERE updated_batch_id = $b AND arrival_batch_id <> $b),
                inferred_items_total          = (SELECT count(*) FROM gold.dim_item_current WHERE is_inferred),
                incomplete_items_total        = (SELECT count(*) FROM gold.dim_item_current WHERE NOT is_complete),
                unknown_category_items_total  = (SELECT count(*) FROM gold.dim_item_current WHERE category_key = '-1' AND NOT is_inferred),
                status = 'success', finished_at = $now, error_message = NULL
            WHERE batch_id = $b
        """, {"b": batch_id, "now": now()}).fetchall()


def mark_failed(cfg, batch_id, error):
    with connect(cfg) as con:
        con.execute("UPDATE ops.batch_log SET status = 'failed', finished_at = ?, error_message = ? WHERE batch_id = ?",
                    [now(), str(error)[:1000], batch_id]).fetchall()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--setup", action="store_true", help="create ops.batch_log")
    group.add_argument("--done", nargs="+", metavar="BATCH_ID", help="record batches that were already processed")
    args = parser.parse_args()
    cfg = load_config()
    if args.setup:
        setup(cfg)
        print("ops.batch_log ready")
    else:
        for b in args.done:
            start(cfg, b)
            finalize(cfg, b)
            print(b, "success")
