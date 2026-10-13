"""Run identity, structured JSON logging and the per-run summary record."""
import json
import os
import re
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from src.config import path_for

_LATEST = 'latest_run_id.txt'


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_run_id(raw: str) -> str:
    return re.sub(r'[^A-Za-z0-9_.-]', '_', raw)


def current_run_id(new: bool = False) -> str:
    """PIPELINE_RUN_ID (set by Airflow to {{ run_id }}) wins; otherwise reuse/create a local id."""
    runs = path_for('runs_dir')
    runs.mkdir(parents=True, exist_ok=True)
    env = os.getenv('PIPELINE_RUN_ID')
    if env:
        rid = safe_run_id(env)
    elif not new and (runs / _LATEST).exists():
        rid = (runs / _LATEST).read_text().strip()
    else:
        rid = f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:6]}"
    (runs / _LATEST).write_text(rid)
    return rid


def log_event(run_id: str, stage: str, status: str, **fields) -> dict:
    rec = {'timestamp': utc_now(), 'pipeline_run_id': run_id, 'stage': stage, 'status': status, **fields}
    path = path_for('runs_dir') / 'pipeline_log.jsonl'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(rec, default=str) + '\n')
    print(f"[{stage}] {status} " + json.dumps(fields, default=str))
    return rec


@contextmanager
def stage(run_id: str, name: str):
    """Times a stage and logs success or the error (then re-raises so the CLI/Airflow sees the failure)."""
    start = time.perf_counter()
    info: dict = {}
    try:
        yield info
    except Exception as exc:
        log_event(run_id, name, 'FAILED', duration_s=round(time.perf_counter() - start, 3),
                  error=f'{type(exc).__name__}: {exc}', **info)
        update_summary(run_id, status='FAILED', failed_stage=name, error=str(exc))
        raise
    log_event(run_id, name, 'OK', duration_s=round(time.perf_counter() - start, 3), **info)


def update_summary(run_id: str, **fields) -> dict:
    """data/runs/run_summary_<run_id>.json: start/end, status and counts per layer."""
    path = path_for('runs_dir') / f'run_summary_{run_id}.json'
    data = json.loads(path.read_text()) if path.exists() else {'pipeline_run_id': run_id, 'started_at_utc': utc_now(), 'status': 'RUNNING', 'counts': {}}
    counts = fields.pop('counts', None)
    if counts:
        data['counts'].update(counts)
    data.update(fields)
    data['updated_at_utc'] = utc_now()
    path.write_text(json.dumps(data, indent=2, default=str))
    return data
