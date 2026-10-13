"""Predict the test split, write metrics.json/predictions.csv and compare with the baseline."""
import json

import joblib
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score

from src.config import SETTINGS, path_for
from src.observability import utc_now
from src.validate import contract


def evaluate(curated: pd.DataFrame, split: dict, run_id: str) -> dict:
    feats = contract()['feature_columns']
    out = path_for('outputs_dir')
    model = joblib.load(out / 'model' / 'model.joblib')
    cfg = json.loads((out / 'model' / 'model_config.json').read_text())
    test_df = curated.set_index('record_id').loc[split['test_ids_in_order']]
    proba = model.predict_proba(test_df[feats])[:, 1]
    pred = model.predict(test_df[feats])
    y = test_df['is_malignant']
    metrics = {'accuracy': round(accuracy_score(y, pred), 4), 'f1': round(f1_score(y, pred), 4),
               'roc_auc': round(roc_auc_score(y, proba), 4)}
    preds = pd.DataFrame({'record_id': test_df.index, 'model_version': cfg['model_version'], 'y_true': y.values,
                          'y_pred': pred, 'proba_malignant': proba.round(6), 'pipeline_run_id': run_id,
                          'predicted_at_utc': utc_now()})
    preds.to_csv(out / 'predictions.csv', index=False)
    result = {'pipeline_run_id': run_id, 'model_version': cfg['model_version'], 'test_rows': len(preds),
              'metrics': metrics, 'evaluated_at_utc': utc_now()}
    (out / 'metrics.json').write_text(json.dumps(result, indent=2))
    return result


def reproducibility_check() -> dict:
    """Machine-readable expected vs actual comparison. Fails if any metric is outside the tolerance."""
    out = path_for('outputs_dir')
    expected = json.loads(path_for('expected_metrics').read_text())
    actual = json.loads((out / 'metrics.json').read_text())
    tol = expected.get('tolerance', SETTINGS['model']['tolerance'])
    rows = {m: {'baseline': v, 'reproduced': actual['metrics'][m], 'abs_diff': round(abs(actual['metrics'][m] - v), 4),
                'within_tolerance': abs(actual['metrics'][m] - v) <= tol}
            for m, v in expected['metrics'].items()}
    passed = all(r['within_tolerance'] for r in rows.values()) and actual['test_rows'] == expected['test_rows']
    comparison = {'primary_metric': expected['primary_metric'], 'tolerance': tol, 'metrics': rows,
                  'test_rows': {'baseline': expected['test_rows'], 'reproduced': actual['test_rows']},
                  'passed': passed, 'pipeline_run_id': actual['pipeline_run_id'], 'checked_at_utc': utc_now()}
    (out / 'metrics_comparison.json').write_text(json.dumps(comparison, indent=2))
    return comparison
