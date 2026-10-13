# Reproducibility report — DSS150P Laboratory Activity 4

**Student:** Claudia González Martín · **Course:** DSS150P Fundamentals of Data Engineering

| Field | Entry |
|---|---|
| Previous predictive project | No group project from last term (Erasmus student, joined Mapúa this term). As agreed with the instructor, a different work is used: a public reference analysis, `baseline/reference_analysis.py` |
| Dataset / source | Breast Cancer Wisconsin (Diagnostic), UCI ML Repository, CC BY 4.0 (copy bundled with scikit-learn 1.5.2) |
| Predictive task | Classification (malignant vs benign) |
| Primary metric | Accuracy (F1 and ROC-AUC also tracked) |

## 1. Baseline ("previous project")
Because I did not take Predictive Analytics last term, I could not reproduce a group project. With the instructor's
agreement, the baseline is a typical notebook-style analysis on a public dataset, kept in `baseline/reference_analysis.py`:
it reads the CSV, uses all 30 measurements, splits once with `train_test_split(test_size=0.2, stratify=y, random_state=42)`,
trains `StandardScaler + LogisticRegression(max_iter=5000)` and prints the metrics. Its output is stored as the
machine-readable baseline `metadata/expected_metrics.json`: **accuracy 0.9649, F1 0.9512, ROC-AUC 0.9960 on 114 test rows.**

### Engineering-gap table
| Current behaviour (baseline) | Risk | DSS150P concept | Remediation in this repo |
|---|---|---|---|
| CSV read from a path inside the script, no identity check | A different file silently changes results | Source contracts | `source_manifest.yml` SHA-256, `verify_source()` fails on mismatch |
| One script, all steps in memory | Cannot rerun one step or see where it failed | Modular code / orchestration | `src/` modules + CLI + Airflow DAG with 10 tasks |
| No schema or quality checks | A renamed/dropped column changes the feature matrix | Data quality | `data_contract.yml` + 6 check categories, quarantine |
| Split exists only in memory | Cannot prove which rows were test rows | Reproducibility | `metadata/split_manifest.json` with ordered ids + hash |
| Metrics only printed | No machine-readable comparison | Delivery / observability | `outputs/metrics.json` + `metrics_comparison.json` |
| No environment definition | Different library versions → different numbers | Reproducible environments | Pinned `requirements.txt`, Dockerfile, Docker Compose |
| No storage layer or audit | No traceability or rerun safety | Storage / ETL | raw → staging → curated, PostgreSQL UPSERT, audit tables |

## 2. Reproduction result
| Metric | Baseline | Reproduced | |Δ| | Within tolerance (0.005) |
|---|---|---|---|---|
| Accuracy (primary) | 0.9649 | 0.9649 | 0.0000 | ✅ |
| F1 | 0.9512 | 0.9512 | 0.0000 | ✅ |
| ROC-AUC | 0.9960 | 0.9960 | 0.0000 | ✅ |
| Test rows | 114 | 114 | 0 | ✅ |

Source: `outputs/metrics_comparison.json` (regenerated on every run by the `reproducibility_check` task; the task fails
the DAG if any metric is outside tolerance). Outcome: **matches within tolerance (exact match)**.

**Deterministic controls:** frozen source + checksum; stable `record_id` from source order; same stratified split and
seed, stored in `split_manifest.json` (sha256 `7f7fc5cb…`) and verified on every run; training rows passed to the model in
the manifest order; fixed feature list/order from the data contract; pinned library versions; `random_state` on the model.

**Deviations from the baseline:** none in data, split or model. The pipeline adds layers, audit columns and a
quarantine; no row was quarantined, so the curated dataset has exactly the 569 baseline rows.

**Known limitations:** the baseline is not a real group project; the dataset is small (569 rows), so storage timings are
dominated by fixed overhead; `record_id` is assigned from the source order (the scikit-learn copy has no original UCI ID);
the incremental ingestion is a laboratory simulation (no business timestamp).

## 3. Evidence of runs (my machine)
See `docs/screenshots/` and `docs/RUN_EVIDENCE.md`: successful Airflow run, intentionally failed run
(`inject_bad_data=true` stops at `validate_raw`, nothing reaches the model) and the recovered run, rerun without duplicates,
and test results.

## 4. Analysis and reflection questions
1. **What prevented immediate reproduction?** The analysis was a single script/notebook: hidden in-memory state, a file path
   inside the code, no record of which rows were in the test set, no pinned library versions and metrics only printed to the screen.
2. **Authoritative source.** The original file in `data/source/` (identified by its SHA-256) is the authority. The raw copy
   is an audited, run-specific snapshot of it (proof of what each run received). The curated dataset is the trusted,
   validated version for consumers and the database. The model-ready view (curated rows + split manifest) is derived and
   can always be rebuilt; it is never edited by hand.
3. **Duplicate prevention.** `record_id` is the business key (PostgreSQL primary key, UPSERT `ON CONFLICT (record_id)`),
   plus `row_hash`/`record_hash` to detect re-deliveries and real changes. Assumption: the source order is immutable, so
   the same row always gets the same `record_id`; the checksum guarantees that assumption.
4. **Incremental without a timestamp.** The source is split into 4 deterministic batches by `record_id`; the checkpoint stores
   the last committed batch number, not a date. Files and docs call it a "batch checkpoint (laboratory simulation)", never a business watermark.
5. **Staging vs curated.** Staging: technical cleaning of one source (types, normalising the label, one row per key,
   quarantine of invalid rows). Curated: business/model meaning (binary target, split assignment, record hash,
   model-ready column order). Staging can be reused for other analyses; curated is shaped for this consumer.
6. **Best storage format.** Parquet was the smallest and fastest file format; PostgreSQL was fastest for the filtered query.
   With 569 rows everything is cached and overhead dominates, so the result does not generalise to large or remote data.
7. **Retry after a commit without confirmation.** Airflow sees the task as failed and runs it again, so the same rows are
   sent to PostgreSQL a second time.
8. **Why that retry is safe.** The load is an UPSERT on `record_id` that only updates when `record_hash` changed, inside one
   transaction; the second attempt inserts 0 rows and updates 0 rows. Predictions use `(record_id, model_version)`.
9. **Check most likely to catch silent accuracy damage.** The schema/feature-order check (contract feature list) together
   with the split-manifest hash: a renamed, dropped or reordered column, or a changed split, would still let training "succeed".
10. **Same train/test rows as the baseline.** The split is produced with the same call, seed and row order; the ordered ids and
    their hash are stored in `split_manifest.json`, every run recomputes and compares the hash, and the metrics match exactly.
11. **Remaining nondeterminism.** Floating-point differences across CPUs/BLAS libraries, different library versions if the
    pins are ignored, and thread scheduling in numeric libraries. LogisticRegression with lbfgs is deterministic on the same setup.
12. **Diagnosing a metric difference.** Compare in order: source checksum (data) → staging/curated row counts and quarantine
    (preprocessing) → split-manifest hash (split) → `pip freeze` vs `requirements.txt` (dependencies) → `model_config.json`
    (parameters) → run twice on the same machine (nondeterminism).
13. **Production monitoring.** Run status and duration per task, row counts per layer, quarantine rate, checksum/contract
    failures, class balance drift, prediction distribution, metric vs baseline, and database load statistics.
14. **First bottleneck at 100×.** Pandas loads whole files in memory in each step and the CSV→COPY load is single-threaded;
    moving to chunked/partitioned processing (or Spark) and bulk partitioned loads would be the first change.
15. **If the data became sensitive.** Do not commit it (manifest + acquisition instructions only), encrypt at rest/in
    transit, use a secrets manager, role-based access in PostgreSQL, pseudonymise identifiers, and log access.

**Design decision I would change for production:** replace the file-based checkpoint and local folders with object storage
plus a metadata table in PostgreSQL, so several workers could run safely and the state would be backed up.

## 5. Use of AI tools
In line with the course rules, I acknowledge that I used an AI assistant (Claude, by Anthropic) to help design and write
parts of the code and documentation. I ran the pipeline, Docker and Airflow on my own machine, checked the results and
evidence, and I am responsible for the submission.
