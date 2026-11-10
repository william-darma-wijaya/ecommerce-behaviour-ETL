# Retail Rocket data pipeline

Medallion (bronze → silver → gold) + Kimball star schema over the Retail Rocket clickstream,
loaded incrementally per simulated day.

Stack (all in Docker): Airflow 3.3.2 (LocalExecutor), DuckDB (`data/warehouse.duckdb`),
dbt-duckdb (in `/opt/dbt-venv`), JupyterLab.

## Setup

Requires Docker Desktop (WSL2 backend, at least 4 GB memory). The Kaggle CSVs go in `dataset/`.

```bash
copy .env.example .env                # once; fill in JUPYTER_TOKEN and the Airflow admin credentials
docker compose build                  # after changing Dockerfile or requirements
docker compose up airflow-init        # once (and after a metadata reset)
docker compose up -d                  # start everything
```

- Airflow UI: http://localhost:8080 (user/password from `.env`)
- JupyterLab: http://localhost:8888 (token from `.env`)

## Everyday commands

```bash
docker compose exec airflow-scheduler bash                                   # shell inside the pipeline container
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt debug --project-dir dbt
docker compose down                                                          # stop (data in ./data stays)
```

## Simulator

```bash
docker compose exec airflow-scheduler python -m simulator.prepare_source     # once: copy the 2-week slice -> schema source
docker compose exec airflow-scheduler python -m simulator.emit_batch --init   # reset the clock, deliver INIT
docker compose exec airflow-scheduler python -m simulator.emit_batch --batches 3   # next 3 batches (1 batch = 1 day)
```

Batches land in `data/landing/<source>/batch_id=<id>/` with a manifest in `data/landing/_manifests/`.
Full reset: delete `data/landing` and `data/warehouse.duckdb`, then run the three commands again.

## Bronze ingestion

```bash
docker compose exec airflow-scheduler python -m ingestion.ingest_bronze --setup              # once: create the bronze tables
docker compose exec airflow-scheduler python -m ingestion.ingest_bronze --batch INIT
docker compose exec airflow-scheduler python -m ingestion.ingest_bronze --batch 2015060100
```

## Silver (dbt)

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt run --project-dir dbt --select silver --vars "{batch_id: INIT}"
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt run --project-dir dbt --select silver --vars "{batch_id: 2015060100}"
```

Run batches in delivery order (INIT first). Each source gets a clean table and a `*_quarantine` table (invalid rows and duplicates, with `reasons`).

## Gold (dbt)

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt seed --project-dir dbt
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt run --project-dir dbt --full-refresh --vars "{batch_id: INIT}"
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt run --project-dir dbt --vars "{batch_id: 2015060100}"
```

`dbt run` without `--select` builds silver, the gold dimensions and `fact_event` for that batch. `--full-refresh` only on the first batch.

## Batch log and the daily DAG

`ops.batch_log` has one row per batch (status, attempt, row counts per layer). The DAG `rr_daily`
delivers the next day and processes the oldest batch that is not `success` (one batch per run).

```bash
docker compose exec airflow-scheduler python -m ops.batch_log --setup                  # once
docker compose exec airflow-scheduler python -m ops.batch_log --done INIT 2015060100    # batches already processed by hand
docker compose exec airflow-scheduler airflow pools set duckdb_pool 1 "DuckDB single writer"   # once
docker compose exec airflow-scheduler airflow dags test rr_daily                        # one run in the terminal
```

Or trigger `rr_daily` in the Airflow UI. Close notebook connections first: DuckDB allows only one writer.

## Exploring the warehouse from a notebook

```python
import duckdb
con = duckdb.connect("/opt/project/data/warehouse.duckdb", read_only=True)
con.sql("SHOW ALL TABLES").show()
con.close()   # close before the pipeline runs again: DuckDB allows only one writer
```
