"""Full and incremental (checkpointed) ingestion of the immutable source file into the raw layer."""
import hashlib
import json
import math
import shutil
from pathlib import Path

import pandas as pd

from src.config import SETTINGS, load_yaml, path_for
from src.observability import utc_now


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def verify_source(source: Path | None = None) -> str:
    """Fail fast if the source snapshot is not the one declared in metadata/source_manifest.yml."""
    source = source or path_for('source_file')
    if not source.exists():
        raise FileNotFoundError(f'Source file not found: {source}. Place it as described in the README.')
    expected = load_yaml('manifest')['sha256']
    actual = sha256_of(source)
    if actual != expected:
        raise ValueError(f'Checksum mismatch for {source.name}: expected {expected[:12]}..., got {actual[:12]}...')
    return actual


def row_hash(row: pd.Series) -> str:
    return hashlib.sha256('|'.join(str(v) for v in row.values).encode()).hexdigest()


def ingest_full(run_id: str, source: Path | None = None, verify: bool = True) -> dict:
    """Copy the source bytes unchanged into data/raw/run_id=<id>/ plus an audit metadata file."""
    source = source or path_for('source_file')
    checksum = verify_source(source) if verify else sha256_of(source)
    out_dir = path_for('raw_dir') / f'run_id={run_id}'
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / source.name
    shutil.copyfile(source, target)
    rows = sum(1 for _ in target.open(encoding='utf-8')) - 1
    meta = {'pipeline_run_id': run_id, 'ingested_at_utc': utc_now(), 'source_file': source.name,
            'source_checksum': checksum, 'raw_file': str(target.relative_to(path_for('raw_dir').parent.parent)),
            'row_count': rows, 'mode': 'full'}
    (out_dir / '_ingest_metadata.json').write_text(json.dumps(meta, indent=2))
    return meta


def _load_checkpoint() -> dict:
    cp = path_for('checkpoint_file')
    if cp.exists():
        return json.loads(cp.read_text())
    return {'last_committed_batch': -1, 'committed': []}


def _save_checkpoint(state: dict) -> None:
    cp = path_for('checkpoint_file')
    cp.parent.mkdir(parents=True, exist_ok=True)
    tmp = cp.with_suffix('.tmp')
    tmp.write_text(json.dumps(state, indent=2))
    tmp.replace(cp)  # atomic: a crash never leaves a half-written checkpoint


def ingest_incremental(run_id: str, max_batches: int | None = None, force: bool = False) -> dict:
    """Simulated arrivals: the source is split into N deterministic batches by record_id.

    Each committed batch is written once to data/raw/batches/batch_id=XX.csv and recorded in the
    checkpoint (outside the code). A rerun skips committed batches unless force=True (full refresh).
    """
    checksum = verify_source()
    df = pd.read_csv(path_for('source_file')).sort_values('record_id').reset_index(drop=True)
    n = SETTINGS['ingestion']['batch_count']
    size = math.ceil(len(df) / n)
    df['batch_id'] = (df['record_id'] - 1) // size
    batch_dir = path_for('batch_dir')
    if force:
        shutil.rmtree(batch_dir, ignore_errors=True)
        state = {'last_committed_batch': -1, 'committed': []}
    else:
        state = _load_checkpoint()
    batch_dir.mkdir(parents=True, exist_ok=True)
    written, skipped = [], []
    for b in range(n):
        if b <= state['last_committed_batch']:
            skipped.append(b)
            continue
        if max_batches is not None and len(written) >= max_batches:
            break
        part = df[df['batch_id'] == b].drop(columns='batch_id').copy()
        part['row_hash'] = part.apply(row_hash, axis=1)
        part['pipeline_run_id'] = run_id
        part['ingested_at_utc'] = utc_now()
        part['source_file'] = path_for('source_file').name
        part['source_checksum'] = checksum
        part['batch_id'] = b
        tmp = batch_dir / f'batch_id={b:02d}.csv.tmp'
        part.to_csv(tmp, index=False)
        tmp.replace(batch_dir / f'batch_id={b:02d}.csv')
        state['last_committed_batch'] = b
        state['committed'].append({'batch_id': b, 'rows': len(part), 'run_id': run_id, 'committed_at_utc': utc_now()})
        _save_checkpoint(state)
        written.append(b)
    return {'mode': 'incremental', 'written_batches': written, 'skipped_batches': skipped,
            'last_committed_batch': state['last_committed_batch'], 'batch_count': n}


def read_raw(run_id: str, mode: str) -> pd.DataFrame:
    """Raw layer as a DataFrame: the run snapshot (full) or the union of committed batches (incremental)."""
    if mode == 'incremental':
        files = sorted(path_for('batch_dir').glob('batch_id=*.csv'))
        if not files:
            raise FileNotFoundError('No committed batches. Run: python -m src.cli ingest --mode incremental')
        return pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    snap = path_for('raw_dir') / f'run_id={run_id}' / path_for('source_file').name
    if not snap.exists():
        raise FileNotFoundError(f'Raw snapshot for run {run_id} not found. Run the ingest step first.')
    return pd.read_csv(snap)
