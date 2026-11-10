"""Load one delivered batch from data/landing into bronze.

All files of a batch load in one transaction: if a file is missing or its row count differs
from the manifest, nothing is loaded. Rerunning a batch replaces its rows.
"""
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml

TABLES = {  # landing source -> (bronze table, business columns)
    "events": ("brz_events", "timestamp, visitorid, event, itemid, transactionid"),
    "item_properties": ("brz_item_properties", "timestamp, itemid, property, value"),
    "category_tree": ("brz_category_tree", "categoryid, parentid"),
}


def load_config():
    with open(os.environ.get("PIPELINE_CONFIG", "config/pipeline.yml")) as f:
        return yaml.safe_load(f)


def setup(cfg):
    ddl = (Path(__file__).parent / "bronze_ddl.sql").read_text()
    with duckdb.connect(cfg["paths"]["warehouse_db"]) as con:
        con.execute(ddl).fetchall()


def ingest(cfg, batch_id):
    landing = Path(cfg["paths"]["landing_dir"])
    manifest_path = landing / "_manifests" / f"{batch_id}.json"
    if not manifest_path.exists():
        raise SystemExit(f"No manifest for batch {batch_id}: the batch is not (fully) delivered yet")
    manifest = json.loads(manifest_path.read_text())
    for f in manifest["files"]:
        if not (landing / f["path"]).exists():
            raise SystemExit(f"File listed in the manifest is missing: {f['path']}")

    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)   # naive UTC, whole seconds
    counts = {}
    with duckdb.connect(cfg["paths"]["warehouse_db"]) as con:
        con.execute("SET enable_progress_bar = false").fetchall()
        if con.execute("SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'bronze'").fetchall()[0][0] < 3:
            raise SystemExit("Bronze tables are missing: run  python -m ingestion.ingest_bronze --setup  first")

        con.execute("BEGIN TRANSACTION").fetchall()
        try:
            for f in manifest["files"]:
                table, cols = TABLES[f["source"]]
                con.execute(f"DELETE FROM bronze.{table} WHERE source_file = ?", [f["path"]]).fetchall()
                # row_number() OVER () follows the file's line order (DuckDB keeps insertion order)
                rows = con.execute(f"""
                    INSERT INTO bronze.{table}
                    SELECT {cols}, ?, ?::TIMESTAMP, ?, ?, row_number() OVER ()
                    FROM read_csv('{landing / f["path"]}', all_varchar = true, header = true)
                """, [batch_id, manifest["window_end"], ingested_at, f["path"]]).fetchall()[0][0]
                if rows != f["rows"]:
                    raise ValueError(f"{f['path']}: manifest says {f['rows']} rows, file has {rows}")
                counts[f["source"]] = rows
            con.execute("COMMIT").fetchall()
        except Exception:
            con.execute("ROLLBACK").fetchall()
            raise
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--setup", action="store_true", help="create the bronze schema and tables")
    group.add_argument("--batch", help="batch_id to load, e.g. INIT or 2015060100")
    args = parser.parse_args()
    cfg = load_config()
    if args.setup:
        setup(cfg)
        print("bronze tables ready")
    else:
        print(args.batch, ingest(cfg, args.batch))
