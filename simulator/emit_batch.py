"""Deliver batches into the landing folder.

A batch is every source row with timestamp in (sim_now, sim_now + step_hours]. INIT is every row
up to simulation.start plus the whole category tree. Empty files still get a header.
"""
import argparse
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from simulator import clock

COLUMNS = {
    "events": "timestamp, visitorid, event, itemid, transactionid",
    "item_properties": "timestamp, itemid, property, value",
    "category_tree": "categoryid, parentid",
}


def in_window(source, window_start, window_end):
    """SQL condition selecting the rows of one source for a batch (window_start None = INIT)."""
    if source == "category_tree":        # no timestamp: the whole tree is known from the start
        return "true" if window_start is None else "false"
    t = "epoch_ms(timestamp::BIGINT)"
    where = f"{t} <= TIMESTAMP '{window_end}'"
    if window_start is not None:
        where += f" AND {t} > TIMESTAMP '{window_start}'"
    return where


def emit(con, cfg, batch_id, window_start, window_end):
    """Write the landing files and manifest for one batch (window_start None = INIT)."""
    landing = Path(cfg["paths"]["landing_dir"])
    files = []
    for source, cols in COLUMNS.items():
        rel = f"{source}/batch_id={batch_id}/{source}_{batch_id}.csv"
        (landing / rel).parent.mkdir(parents=True, exist_ok=True)
        rows = con.execute(f"""
            COPY (SELECT {cols} FROM source.{source}
                  WHERE {in_window(source, window_start, window_end)} ORDER BY ALL)
            TO '{landing / rel}' (HEADER)""").fetchall()[0][0]
        files.append({"source": source, "path": rel, "rows": rows})

    manifest = {
        "batch_id": batch_id,
        "window_start": window_start.isoformat() if window_start else None,
        "window_end": window_end.isoformat(),
        "files": files,
    }
    path = landing / "_manifests" / f"{batch_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2))
    os.replace(tmp, path)       # appears all at once: a manifest is never half-written
    return manifest


def emit_init(cfg):
    with clock.connect(cfg) as con:
        clock.reset(con, cfg)
        return emit(con, cfg, "INIT", None, clock.get_now(con))


def emit_next_batch(cfg):
    """Deliver the next batch (one step) and advance the clock. Returns None once simulation.end is reached."""
    end = datetime.fromisoformat(cfg["simulation"]["end"])
    with clock.connect(cfg) as con:
        now = clock.get_now(con)
        if now >= end:
            return None
        window_end = now + timedelta(hours=cfg["simulation"]["step_hours"])
        manifest = emit(con, cfg, now.strftime("%Y%m%d%H"), now, window_end)
        clock.set_now(con, window_end)   # after the files: a crash before this just re-emits the same batch
        return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--init", action="store_true", help="reset the clock and deliver INIT")
    group.add_argument("--batches", type=int, help="number of batches to deliver")
    args = parser.parse_args()
    cfg = clock.load_config()
    for _ in range(1 if args.init else args.batches):
        m = emit_init(cfg) if args.init else emit_next_batch(cfg)
        if m is None:
            print("simulation.end reached")
            break
        print(m["batch_id"], {f["source"]: f["rows"] for f in m["files"]})
