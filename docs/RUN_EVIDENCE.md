# Run evidence (my machine)

Screenshots in [screenshots/](screenshots/).

| # | Run | Config | Result | Screenshot |
|---|---|---|---|---|
| 1 | Pipeline via CLI | `run-all` | success, reproducibility passed | `01_run_all.png` |
| 2 | Airflow successful run | `{"ingest_mode": "full"}` | success (10 tasks green) | `02_airflow_success.png` |
| 3 | Airflow intentional failure | `{"inject_bad_data": "true"}` | failed at `validate_raw`; downstream not run | `03_airflow_failed.png` |
| 4 | Airflow recovery | `{"ingest_mode": "full"}` | success | `03_airflow_failed.png` (same grid) |
| 5 | Second run, no duplicates | `SELECT count(*), count(DISTINCT record_id) ...` | 569 / 569 | `04_no_duplicates.png` |
| 6 | Tests | `pytest -v` | all passed | `05_tests.png` |
| 7 | Git history | `git log --oneline` | milestone commits | `06_git_log.png` |
