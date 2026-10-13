"""PostgreSQL access: schema creation, rerun-safe UPSERT loads and the audit run record."""
import io

import pandas as pd
import psycopg

from src.config import DB, ROOT


def connect():
    if not DB['password']:
        raise RuntimeError('POSTGRES_PASSWORD is not set. Copy .env.example to .env and set a password.')
    return psycopg.connect(**DB, connect_timeout=5)


def init_db() -> None:
    with connect() as conn:
        conn.execute((ROOT / 'sql' / 'schema.sql').read_text())
        conn.execute((ROOT / 'sql' / 'transformations.sql').read_text())


def _copy_to_temp(conn, df: pd.DataFrame, temp: str, like: str) -> None:
    conn.execute(f'CREATE TEMP TABLE {temp} (LIKE {like} INCLUDING DEFAULTS) ON COMMIT DROP')
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cols = ','.join(df.columns)
    with conn.cursor().copy(f'COPY {temp} ({cols}) FROM STDIN WITH (FORMAT csv)') as cp:
        cp.write(buf.read())


def upsert_curated(df: pd.DataFrame) -> dict:
    """INSERT ... ON CONFLICT (record_id) DO UPDATE only when record_hash changed -> rerun inserts 0 rows."""
    cols = list(df.columns)
    updates = ', '.join(f'{c} = EXCLUDED.{c}' for c in cols if c != 'record_id')
    with connect() as conn:
        before = conn.execute('SELECT count(*) FROM curated.breast_cancer_features').fetchone()[0]
        _copy_to_temp(conn, df, 'tmp_features', 'curated.breast_cancer_features')
        cur = conn.execute(
            f'INSERT INTO curated.breast_cancer_features ({",".join(cols)}) SELECT {",".join(cols)} FROM tmp_features '
            f'ON CONFLICT (record_id) DO UPDATE SET {updates} '
            'WHERE curated.breast_cancer_features.record_hash IS DISTINCT FROM EXCLUDED.record_hash')
        affected = cur.rowcount
        after = conn.execute('SELECT count(*) FROM curated.breast_cancer_features').fetchone()[0]
    return {'input_rows': len(df), 'inserted': after - before, 'updated': affected - (after - before),
            'unchanged': len(df) - affected, 'table_rows': after}


def upsert_predictions(df: pd.DataFrame) -> int:
    cols = list(df.columns)
    with connect() as conn:
        _copy_to_temp(conn, df, 'tmp_pred', 'curated.predictions')
        cur = conn.execute(
            f'INSERT INTO curated.predictions ({",".join(cols)}) SELECT {",".join(cols)} FROM tmp_pred '
            'ON CONFLICT (record_id, model_version) DO UPDATE SET y_pred = EXCLUDED.y_pred, '
            'proba_malignant = EXCLUDED.proba_malignant, pipeline_run_id = EXCLUDED.pipeline_run_id, '
            'predicted_at_utc = EXCLUDED.predicted_at_utc')
        return cur.rowcount


def record_run(run_id: str, **fields) -> None:
    """Insert or update this run's row in audit.pipeline_runs (best effort for status fields)."""
    cols = ['pipeline_run_id'] + list(fields)
    sets = ', '.join(f'{k} = EXCLUDED.{k}' for k in fields) or 'status = EXCLUDED.status'
    with connect() as conn:
        conn.execute(f'INSERT INTO audit.pipeline_runs ({",".join(cols)}) VALUES ({",".join(["%s"] * len(cols))}) '
                     f'ON CONFLICT (pipeline_run_id) DO UPDATE SET {sets}', [run_id, *fields.values()])


def query(sql: str, params=None) -> list[tuple]:
    with connect() as conn:
        return conn.execute(sql, params).fetchall()
