# Run evidence (my machine)

Executed on my laptop with Docker Compose (PostgreSQL 16, Apache Airflow 2.10.5, Python 3.11). Screenshots in [screenshots/](screenshots/).

| # | Run | Config / command | Result | Screenshot |
|---|---|---|---|---|
| 1 | Pipeline via CLI | `docker compose run --rm pipeline python -m src.cli run-all` | success, reproducibility check passed | `01_run_all.png` |
| 2 | Airflow successful run | `manual__2026-10-13T14:37:55+00:00` · `{"ingest_mode": "full"}` | success, 10 tasks green | `02_airflow_success.png` |
| 3 | Airflow intentional failure | `manual__2026-10-13T14:42:07+00:00` · `{"inject_bad_data": "true"}` | failed at `validate_raw` (temporary copy without `mean_area`); all downstream tasks upstream_failed, so bad data never reached the model | `03_airflow_failed.png` |
| 4 | Airflow recovery | `manual__2026-10-13T14:43:52+00:00` · `{"inject_bad_data": "false"}` | success (green / red / green in the grid) | `06_airflow_recuperation.png` |
| 5 | Second run, no duplicates | `SELECT count(*), count(DISTINCT record_id) FROM curated.breast_cancer_features;` | 569 / 569 | `04_no_duplicates.png` |
| 6 | Tests | `docker compose run --rm pipeline python -m pytest -v` | all passed | `05_tests.png` |
| 7 | Git history | `git log --oneline` | milestone commits | `07_git_log.png` |

Machine-readable evidence committed with the repository: `outputs/metrics_comparison.json` (`"passed": true`, run
`manual__2026-10-13T14:43:52+00:00`), `outputs/metrics.json`, `outputs/predictions.csv`, `outputs/model/model_config.json`,
`metadata/split_manifest.json`.
