# Runbook

## Normal execution
1. `Copy-Item .env.example .env` (PowerShell) or `cp .env.example .env`, and set `POSTGRES_PASSWORD`.
2. `docker compose up -d postgres`
3. Pipeline only: `docker compose run --rm pipeline python -m src.cli run-all`
4. With Airflow: `docker compose up airflow-init`, then `docker compose up -d airflow-webserver airflow-scheduler`,
   open http://localhost:8081 (admin/admin), unpause `predictive_repro_pipeline`, **Trigger DAG w/ config**.
5. Check `outputs/metrics_comparison.json` (`"passed": true`) and `audit.pipeline_runs` (status `SUCCESS`).

## Common failures
| Symptom | Cause | Fix |
|---|---|---|
| `Checksum mismatch` in ingest_source | The source CSV was modified or replaced | Restore the original file (checksum in `metadata/source_manifest.yml`) |
| `Raw data violates the contract: missing required column` | Schema change / bad copy (or `inject_bad_data=true`) | Fix the source, re-trigger with `inject_bad_data=false` |
| `POSTGRES_PASSWORD is not set` | `.env` missing | Copy `.env.example` to `.env` |
| `connection refused` in load_postgres | Database container down | `docker compose up -d postgres`; the task retries twice automatically |
| `Reproduced metrics are outside the declared tolerance` | Different seed, split, data or library versions | Compare `outputs/metrics_comparison.json` and `metadata/split_manifest.json`; reinstall pinned `requirements.txt` |
| `Split differs from metadata/split_manifest.json` | Non-deterministic split for the same seed | Check library versions and that rows were not reordered/removed |
| `Permission denied` deleting files under data/ | Files created by another container user | Containers share `AIRFLOW_UID`; if it happens, delete the generated folder (`data/staging`, `data/curated`...) and re-run |

## Restart / rerun
All steps are rerun-safe: raw is written to a run-specific folder, staging/curated files are replaced atomically,
PostgreSQL loads are UPSERTs on `record_id`, predictions are UPSERTs on `(record_id, model_version)`.
In Airflow, **Clear** the failed task (and downstream) or trigger a new run. No manual database cleanup is needed.

## Incremental checkpoint recovery
- State: `data/checkpoints/ingest_checkpoint.json` (`last_committed_batch`, list of committed batches).
- A batch file is written to a `.tmp` name and renamed, then the checkpoint is replaced atomically, so a crash leaves
  either the old or the new state. Rerun `python -m src.cli ingest --mode incremental`: committed batches are skipped.
- Full refresh: `python -m src.cli ingest --mode incremental --force` (deletes batches and resets the checkpoint).

## Database reset
`docker compose down -v` (deletes the volume; init scripts run again on next start), or
`DROP SCHEMA curated CASCADE; DROP SCHEMA audit CASCADE;` then `python -m src.cli init-db`.

## Rollback / cleanup
Generated folders (`data/raw`, `data/staging`, `data/curated`, `data/partitioned`, `data/quarantine`, `data/runs`,
`data/checkpoints`, `data/tmp`) can be deleted at any time; the pipeline rebuilds them from `data/source/`.
To roll back a model, re-run with the previous commit: outputs are fully regenerated from curated data.
