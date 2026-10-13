"""Integration tests across module, file and database boundaries."""
import json

import pandas as pd
import pytest

from src import cli
from src.config import path_for
from tests.conftest import db_available


def test_incremental_rerun_adds_no_duplicates(tmp_path, monkeypatch):
    """ingest (incremental, checkpointed) -> raw batches: a rerun skips committed batches."""
    import src.ingest as ing
    monkeypatch.setattr(ing, 'path_for', lambda k: tmp_path / k if k in ('batch_dir', 'checkpoint_file') else path_for(k))
    first = ing.ingest_incremental('it_run_1', max_batches=2)
    second = ing.ingest_incremental('it_run_2')
    third = ing.ingest_incremental('it_run_3')
    raw = pd.concat([pd.read_csv(f) for f in sorted((tmp_path / 'batch_dir').glob('*.csv'))])
    assert first['written_batches'] == [0, 1] and second['written_batches'] == [2, 3]
    assert third['written_batches'] == [] and third['skipped_batches'] == [0, 1, 2, 3]
    assert len(raw) == 569 and raw['record_id'].is_unique


def test_bad_source_copy_never_reaches_model(monkeypatch):
    """Failure injection: a copy without a required column is stopped at validate-raw."""
    monkeypatch.setenv('PIPELINE_RUN_ID', 'it_bad_copy')
    monkeypatch.setenv('INJECT_BAD_DATA', 'true')
    monkeypatch.setenv('INGEST_MODE', 'full')
    cli.main(['ingest'])
    with pytest.raises(ValueError, match='mean_area'):
        cli.main(['validate-raw'])
    rep = json.loads((path_for('runs_dir') / 'validate_raw_it_bad_copy.json').read_text())
    assert not rep['ok']


@pytest.mark.skipif(not db_available(), reason='PostgreSQL not reachable')
def test_end_to_end_rerun_is_idempotent_and_reproducible(monkeypatch):
    """Full pipeline twice: same metrics within tolerance and no duplicate business keys in PostgreSQL."""
    from src.db import query
    monkeypatch.delenv('PIPELINE_RUN_ID', raising=False)
    monkeypatch.setenv('INGEST_MODE', 'full')
    for _ in range(2):
        cli.main(['run-all'])
    total, distinct = query('SELECT count(*), count(DISTINCT record_id) FROM curated.breast_cancer_features')[0]
    assert total == distinct == 569
    comp = json.loads((path_for('outputs_dir') / 'metrics_comparison.json').read_text())
    assert comp['passed']
