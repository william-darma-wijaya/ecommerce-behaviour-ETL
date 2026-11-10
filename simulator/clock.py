"""Simulated clock (source.sim_clock) and config loading shared by the simulator."""
import os
from datetime import datetime

import duckdb
import yaml


def load_config(path=None):
    with open(path or os.environ.get("PIPELINE_CONFIG", "config/pipeline.yml")) as f:
        return yaml.safe_load(f)


def connect(cfg):
    con = duckdb.connect(cfg["paths"]["warehouse_db"])
    con.execute("SET enable_progress_bar = false").fetchall()
    con.execute("CREATE SCHEMA IF NOT EXISTS source").fetchall()
    return con


def get_now(con):
    return con.execute("SELECT sim_now FROM source.sim_clock").fetchall()[0][0]


def set_now(con, ts):
    con.execute("CREATE TABLE IF NOT EXISTS source.sim_clock (sim_now TIMESTAMP)").fetchall()
    con.execute("DELETE FROM source.sim_clock").fetchall()
    con.execute("INSERT INTO source.sim_clock VALUES (?)", [ts]).fetchall()


def reset(con, cfg):
    set_now(con, datetime.fromisoformat(cfg["simulation"]["start"]))
