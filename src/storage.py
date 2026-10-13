"""Storage experiment (CSV / JSON Lines / Parquet / PostgreSQL) and the partitioned dataset."""
import shutil
import statistics
import time

import pandas as pd

from src.config import SETTINGS, path_for
from src.db import connect


def _median_time(fn, repeats: int) -> float:
    times = []
    for _ in range(repeats):
        t = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t)
    return round(statistics.median(times), 4)


def run_benchmark(curated: pd.DataFrame, repeats: int | None = None) -> pd.DataFrame:
    """Same logical records in every format. Filtered read = malignant rows only."""
    repeats = repeats or SETTINGS['benchmark']['repeats']
    d = path_for('benchmark_dir') / 'files'
    d.mkdir(parents=True, exist_ok=True)
    csv_p, json_p, pq_p = d / 'curated.csv', d / 'curated.jsonl', d / 'curated.parquet'
    rows = []
    fmts = {
        'CSV': (lambda: curated.to_csv(csv_p, index=False), lambda: pd.read_csv(csv_p),
                lambda: pd.read_csv(csv_p).query("diagnosis == 'M'"), csv_p),
        'JSON Lines': (lambda: curated.to_json(json_p, orient='records', lines=True), lambda: pd.read_json(json_p, lines=True),
                       lambda: pd.read_json(json_p, lines=True).query("diagnosis == 'M'"), json_p),
        'Parquet': (lambda: curated.to_parquet(pq_p, index=False), lambda: pd.read_parquet(pq_p),
                    lambda: pd.read_parquet(pq_p, filters=[('diagnosis', '==', 'M')]), pq_p),
    }
    for name, (w, r, fr, p) in fmts.items():
        wt = _median_time(w, repeats)
        rows.append({'storage_type': name, 'size_bytes': p.stat().st_size, 'write_s': wt,
                     'full_read_s': _median_time(r, repeats), 'filtered_read_s': _median_time(fr, repeats),
                     'rows': len(r())})
    try:
        with connect() as conn:
            def pg_full():
                return conn.execute('SELECT * FROM curated.breast_cancer_features').fetchall()

            def pg_filtered():
                return conn.execute("SELECT * FROM curated.breast_cancer_features WHERE diagnosis = 'M'").fetchall()
            size = conn.execute("SELECT pg_total_relation_size('curated.breast_cancer_features')").fetchone()[0]
            rows.append({'storage_type': 'PostgreSQL', 'size_bytes': size, 'write_s': None,
                         'full_read_s': _median_time(pg_full, repeats), 'filtered_read_s': _median_time(pg_filtered, repeats),
                         'rows': len(pg_full())})
    except Exception as exc:  # benchmark still useful for files if the DB is down
        print('PostgreSQL benchmark skipped:', exc)
    out = pd.DataFrame(rows)
    out.to_csv(path_for('benchmark_dir') / 'storage_benchmark.csv', index=False)
    return out


def write_partitions(curated: pd.DataFrame) -> list[str]:
    """Hive-style partitions by diagnosis (the only natural low-cardinality column of this static dataset)."""
    col = SETTINGS['benchmark']['partition_column']
    root = path_for('partition_dir') / 'breast_cancer_features'
    written = []
    for value, part in curated.groupby(col):
        d = root / f'{col}={value}'
        if d.exists():
            for f in d.glob('*.parquet'):
                f.unlink(missing_ok=True)  # replace only our own files (no recursive delete of shared dirs)
        d.mkdir(parents=True, exist_ok=True)
        part.drop(columns=col).to_parquet(d / 'part-000.parquet', index=False)
        written.append(str(d.relative_to(path_for('partition_dir'))))
    return written


def read_partition(value: str) -> pd.DataFrame:
    col = SETTINGS['benchmark']['partition_column']
    d = path_for('partition_dir') / 'breast_cancer_features' / f'{col}={value}'
    if not d.exists():
        raise FileNotFoundError(f'Partition {col}={value} not found. Run build-curated first.')
    df = pd.read_parquet(d)
    df[col] = value
    return df
