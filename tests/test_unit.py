"""Unit tests for reusable functions."""
import pandas as pd

from src.ingest import sha256_of, verify_source
from src.config import load_yaml, path_for
from src.transform import make_split, record_hash, to_staging
from src.validate import check_business_rule, check_schema, check_uniqueness, check_validity, contract


def test_source_checksum_matches_manifest():
    assert verify_source() == load_yaml('manifest')['sha256'] == sha256_of(path_for('source_file'))


def test_staging_types_dedup_and_quarantine(source_df):
    raw = pd.concat([source_df, source_df.iloc[[0]]], ignore_index=True)  # duplicate delivery of record 1
    raw.loc[5, 'mean_area'] = -1.0                                         # invalid measurement
    raw.loc[7, 'diagnosis'] = 'X'                                          # invalid category
    staging, quarantine = to_staging(raw, 'test_run')
    assert staging['record_id'].is_unique
    assert len(staging) == len(source_df) - 2
    assert set(quarantine['reason']) == {'negative_measurement', 'invalid_diagnosis'}
    assert staging['mean_area'].dtype == 'float64'


def test_record_hash_ignores_audit_columns(source_df):
    cols = ['record_id'] + contract()['feature_columns'] + ['diagnosis']
    a = source_df.iloc[0].copy()
    b = a.copy()
    b['pipeline_run_id'] = 'another_run'
    assert record_hash(a, cols) == record_hash(b, cols)


def test_split_is_deterministic_and_disjoint(source_df):
    s1, s2 = make_split(source_df, 42, 0.2), make_split(source_df, 42, 0.2)
    assert s1['sha256'] == s2['sha256']
    assert not set(s1['train_ids_in_order']) & set(s1['test_ids_in_order'])
    assert s1['test_rows'] == 114


def test_quality_checks_detect_invalid_conditions(source_df):
    bad = source_df.drop(columns=['mean_area'])
    assert any('missing required column: mean_area' in p for p in check_schema(bad))
    dup = pd.concat([source_df, source_df.iloc[[3]]])
    assert check_uniqueness(dup)
    inv = source_df.copy()
    inv.loc[0, 'diagnosis'] = 'Z'
    assert check_validity(inv)
    rule = source_df.copy()
    rule.loc[0, 'worst_radius'] = 0.1
    assert check_business_rule(rule)
