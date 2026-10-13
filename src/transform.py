"""raw -> staging (typed, deduplicated, quarantined) -> curated (model-ready with deterministic split)."""
import hashlib
import json

import pandas as pd
from sklearn.model_selection import train_test_split

from src.config import SETTINGS, path_for
from src.observability import utc_now
from src.validate import check_business_rule, contract

AUDIT_COLS = ['pipeline_run_id', 'ingested_at_utc', 'source_file', 'source_checksum', 'batch_id', 'row_hash']


def to_staging(raw: pd.DataFrame, run_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    c = contract()
    feats = c['feature_columns']
    df = raw.copy()
    df.columns = [col.strip().lower() for col in df.columns]
    df['record_id'] = pd.to_numeric(df['record_id'], errors='coerce').astype('Int64')
    for f in feats:
        df[f] = pd.to_numeric(df[f], errors='coerce').astype('float64')
    df['diagnosis'] = df['diagnosis'].astype('string').str.strip().str.upper()
    # Duplicate prevention: one row per record_id (exact re-deliveries collapse to one).
    before = len(df)
    df = df.sort_values('record_id', kind='mergesort').drop_duplicates(subset='record_id', keep='last')
    dup_removed = before - len(df)

    reasons = pd.Series('', index=df.index)
    reasons[df[['record_id', 'diagnosis'] + feats].isna().any(axis=1)] += 'null_required;'
    reasons[(df[feats] < 0).any(axis=1)] += 'negative_measurement;'
    reasons[~df['diagnosis'].isin(['M', 'B'])] += 'invalid_diagnosis;'
    if check_business_rule(df):
        reasons[df['worst_radius'] < df['mean_radius']] += 'worst_lt_mean_radius;'
    bad = reasons != ''
    quarantine = df[bad].assign(reason=reasons[bad].str.rstrip(';'), pipeline_run_id=run_id, quarantined_at_utc=utc_now())
    staging = df[~bad][['record_id'] + feats + ['diagnosis']].copy()
    staging['pipeline_run_id'] = run_id
    staging['staged_at_utc'] = utc_now()
    staging.attrs['duplicates_removed'] = dup_removed
    return staging.reset_index(drop=True), quarantine.reset_index(drop=True)


def record_hash(row: pd.Series, cols: list[str]) -> str:
    """Hash of business columns only (never audit columns), so unchanged rows keep the same hash."""
    return hashlib.sha256('|'.join(f'{row[c]!r}' for c in cols).encode()).hexdigest()


def make_split(staging: pd.DataFrame, seed: int, test_size: float) -> dict:
    """Same call as the baseline: stratified train_test_split over rows in source order."""
    y = (staging['diagnosis'] == 'M').astype(int)
    train_ids, test_ids = train_test_split(staging['record_id'].astype(int).tolist(), test_size=test_size,
                                           stratify=y, random_state=seed)
    digest = hashlib.sha256((','.join(map(str, train_ids)) + '|' + ','.join(map(str, test_ids))).encode()).hexdigest()
    return {'seed': seed, 'test_size': test_size, 'stratify': 'diagnosis', 'train_ids_in_order': train_ids,
            'test_ids_in_order': test_ids, 'train_rows': len(train_ids), 'test_rows': len(test_ids), 'sha256': digest}


def to_curated(staging: pd.DataFrame, run_id: str, seed: int | None = None) -> tuple[pd.DataFrame, dict]:
    feats = contract()['feature_columns']
    seed = SETTINGS['model']['seed'] if seed is None else seed
    split = make_split(staging.sort_values('record_id'), seed, SETTINGS['model']['test_size'])
    cur = staging.sort_values('record_id').reset_index(drop=True)[['record_id'] + feats + ['diagnosis']].copy()
    cur['is_malignant'] = (cur['diagnosis'] == 'M').astype(int)
    test_set = set(split['test_ids_in_order'])
    cur['split'] = cur['record_id'].map(lambda r: 'test' if r in test_set else 'train')
    cur['record_hash'] = cur.apply(record_hash, axis=1, cols=['record_id'] + feats + ['diagnosis'])
    cur['pipeline_run_id'] = run_id
    cur['processed_at_utc'] = utc_now()
    return cur, split


def write_layer(df: pd.DataFrame, key: str, name: str) -> str:
    out_dir = path_for(key)
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = out_dir / f'{name}.parquet.tmp'
    df.to_parquet(tmp, index=False)
    final = out_dir / f'{name}.parquet'
    tmp.replace(final)  # overwrite atomically: rerunning a step never leaves partial files
    return str(final)


def check_or_write_split_manifest(split: dict) -> str:
    """First run writes metadata/split_manifest.json; later runs must produce exactly the same split."""
    path = path_for('split_manifest')
    if path.exists():
        old = json.loads(path.read_text())
        if old['seed'] == split['seed'] and old['sha256'] != split['sha256']:
            raise ValueError('Split differs from metadata/split_manifest.json with the same seed: non-deterministic split')
        return 'verified' if old['sha256'] == split['sha256'] else 'different_seed_not_saved'
    path.write_text(json.dumps(split, indent=1))
    return 'created'
