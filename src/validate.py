"""Data-quality checks driven by metadata/data_contract.yml. Each check returns a list of problems."""
import json

import pandas as pd

from src.config import load_yaml, path_for


def contract() -> dict:
    return load_yaml('contract')


def check_schema(df: pd.DataFrame, c: dict | None = None) -> list[str]:
    c = c or contract()
    problems = [f"missing required column: {col['name']}" for col in c['columns'] if col['name'] not in df.columns]
    for col in c['columns']:
        name = col['name']
        if name in df.columns and col['type'] in ('float', 'integer'):
            bad = pd.to_numeric(df[name], errors='coerce').isna() & df[name].notna()
            if bad.any():
                problems.append(f'type mismatch in {name}: {int(bad.sum())} non-numeric values')
    return problems


def check_completeness(df: pd.DataFrame, c: dict | None = None) -> list[str]:
    c = c or contract()
    return [f"nulls in required field {col['name']}: {int(df[col['name']].isna().sum())}"
            for col in c['columns'] if not col['nullable'] and col['name'] in df.columns and df[col['name']].isna().any()]


def check_uniqueness(df: pd.DataFrame, key: str = 'record_id') -> list[str]:
    dup = int(df[key].duplicated().sum())
    return [f'duplicate {key}: {dup}'] if dup else []


def check_validity(df: pd.DataFrame, c: dict | None = None) -> list[str]:
    c = c or contract()
    problems = []
    for col in c['columns']:
        name = col['name']
        if name not in df.columns:
            continue
        if 'min' in col:
            n = int((pd.to_numeric(df[name], errors='coerce') < col['min']).sum())
            if n:
                problems.append(f'{name} below {col["min"]}: {n} rows')
        if 'allowed' in col:
            n = int((~df[name].isin(col['allowed'])).sum())
            if n:
                problems.append(f'{name} outside {col["allowed"]}: {n} rows')
    return problems


def check_business_rule(df: pd.DataFrame) -> list[str]:
    """Project rule: the 'worst' radius is the mean of the 3 largest cells, so it can never be < the mean radius."""
    if {'worst_radius', 'mean_radius'} <= set(df.columns):
        n = int((df['worst_radius'] < df['mean_radius']).sum())
        return [f'worst_radius < mean_radius in {n} rows'] if n else []
    return []


def check_predictive_integrity(curated: pd.DataFrame, feature_cols: list[str]) -> list[str]:
    problems = []
    train = set(curated.loc[curated['split'] == 'train', 'record_id'])
    test = set(curated.loc[curated['split'] == 'test', 'record_id'])
    if train & test:
        problems.append(f'train/test overlap: {len(train & test)} record_ids')
    leaks = [c for c in feature_cols if c in ('diagnosis', 'is_malignant', 'record_id', 'split')]
    if leaks:
        problems.append(f'target/identifier leakage in features: {leaks}')
    if feature_cols != contract()['feature_columns']:
        problems.append('feature columns/order differ from the data contract')
    return problems


def validate_raw(df: pd.DataFrame) -> dict:
    """Blocking checks on raw: a schema/type problem stops the pipeline before anything reaches the model."""
    problems = check_schema(df)
    return {'ok': not problems, 'problems': problems, 'rows': len(df)}


def validate_curated(df: pd.DataFrame, feature_cols: list[str]) -> dict:
    results = {
        'schema': check_schema(df),
        'completeness': check_completeness(df),
        'uniqueness': check_uniqueness(df),
        'validity': check_validity(df),
        'business_rule': check_business_rule(df),
        'predictive_integrity': check_predictive_integrity(df, feature_cols),
    }
    problems = [p for v in results.values() for p in v]
    return {'ok': not problems, 'problems': problems, 'checks': {k: ('PASS' if not v else 'FAIL') for k, v in results.items()}, 'rows': len(df)}


def write_report(run_id: str, name: str, report: dict) -> None:
    out = path_for('runs_dir') / f'{name}_{run_id}.json'
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
