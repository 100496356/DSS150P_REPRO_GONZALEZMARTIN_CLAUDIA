# Storage benchmark report

Same 569 curated rows (36 columns) in each format. 5 repetitions (3 in the development run below), median reported.
Filtered read = malignant rows only (`diagnosis = 'M'`, 212 rows). Command: `python -m src.cli benchmark`.
The table is regenerated in `data/benchmarks/storage_benchmark.csv` on every run.

| Storage | Size | Write (s) | Full read (s) | Filtered read (s) |
|---|---|---|---|---|
| CSV | 199.8 KB | 0.0212 | 0.0079 | 0.0110 |
| JSON Lines | 571.9 KB | 0.0069 | 0.0181 | 0.0164 |
| Parquet | 193.5 KB | 0.0055 | 0.0047 | 0.0100 |
| PostgreSQL | 294.9 KB (`pg_total_relation_size`) | UPSERT step | 0.0041 | 0.0026 |

*Measured in the development environment (Linux, Python 3.11, local PostgreSQL 16). Timings on another machine will differ.*

**Interpretation.** With only 569 rows, every option answers in a few milliseconds, so the differences are mostly fixed
overhead (opening files, parsing headers, network round-trips) rather than data volume.
- **JSON Lines** is about 3× larger than CSV because every row repeats all 36 key names, and it is the slowest to read.
- **Parquet** is the smallest and fastest file format (columnar, typed, compressed), but the gain over CSV is small at this size.
- **PostgreSQL** gives the fastest filtered read here because the table is small and already cached by the server, and it is
  the only option with constraints, UPSERT and concurrent access. It is the serving layer; Parquet is the analytical file layer.

**Why not generalise.** A 120 KB dataset fits in memory and in the database cache; with millions of rows, I/O and
compression would dominate and the ranking (especially PostgreSQL vs Parquet for full scans) could change.

**Partitioning.** The dataset is a static snapshot without dates, so the only meaningful low-cardinality column is
`diagnosis`. `build-curated` writes `data/partitioned/breast_cancer_features/diagnosis=M|B/`, and
`python -m src.cli load-partition --diagnosis M` reads and UPSERTs only that partition (212 rows) and records it in
`audit.partition_loads`. Partitioning brings no speed benefit at this size; it is kept to show that one partition can be read and loaded independently (A11).
