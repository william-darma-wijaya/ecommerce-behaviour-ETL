"""Deliver the next simulated day, then load the oldest unfinished batch. One batch per run."""
from datetime import timedelta

from airflow.sdk import dag, task

from ingestion.ingest_bronze import ingest
from ops import batch_log
from simulator.clock import load_config
from simulator.emit_batch import emit_next_batch

DBT = "/opt/dbt-venv/bin/dbt"
POOL = "duckdb_pool"   # 1 slot: DuckDB allows a single writer


def dbt_run(select, batch_id):
    return f"{DBT} run --project-dir /opt/project/dbt --select {select} --vars '{{batch_id: {batch_id}}}'"


def on_failure(context):
    """Mark the batch `failed`; the next run picks the same batch again."""
    batch_id = context["ti"].xcom_pull(task_ids="pick_next_batch")
    if batch_id:
        batch_log.mark_failed(load_config(), batch_id, context.get("exception"))


@dag(
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1), "on_failure_callback": on_failure},
    tags=["retailrocket"],
)
def rr_daily():

    @task(pool=POOL)
    def simulate_next_batch():
        """Deliver the next day, unless the pipeline is too far behind or the simulation has ended."""
        cfg = load_config()
        if len(batch_log.not_done(cfg)) >= cfg["simulation"]["max_backlog_batches"]:
            return "backlog full, nothing delivered"
        m = emit_next_batch(cfg)
        return m["batch_id"] if m else "simulation.end reached"

    @task.short_circuit(pool=POOL)
    def pick_next_batch():
        """The oldest batch that is not `success`; None skips the rest of the run."""
        return batch_log.pick_next_batch(load_config())

    @task(pool=POOL)
    def ingest_bronze(batch_id):
        return ingest(load_config(), batch_id)

    @task.bash(pool=POOL)
    def dbt_run_silver(batch_id):
        return dbt_run("silver", batch_id)

    @task.bash(pool=POOL)
    def dbt_run_gold_dims(batch_id):
        return dbt_run("dim_category dim_item", batch_id)

    @task.bash(pool=POOL)
    def dbt_run_gold_fact(batch_id):
        return dbt_run("fact_event", batch_id)

    @task(pool=POOL)
    def finalize_batch(batch_id):
        batch_log.finalize(load_config(), batch_id)

    batch_id = pick_next_batch()
    simulate_next_batch() >> batch_id
    (ingest_bronze(batch_id) >> dbt_run_silver(batch_id) >> dbt_run_gold_dims(batch_id)
     >> dbt_run_gold_fact(batch_id) >> finalize_batch(batch_id))


rr_daily()
