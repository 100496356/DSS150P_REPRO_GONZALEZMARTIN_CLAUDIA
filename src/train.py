"""Train the baseline model family on the curated train split, in the manifest's row order."""
import json

import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from src.config import SETTINGS, path_for
from src.validate import contract


def build_model(seed: int):
    return make_pipeline(StandardScaler(), LogisticRegression(max_iter=SETTINGS['model']['max_iter'], random_state=seed))


def train(curated: pd.DataFrame, split: dict, run_id: str) -> dict:
    feats = contract()['feature_columns']
    by_id = curated.set_index('record_id')
    train_df = by_id.loc[split['train_ids_in_order']]
    model = build_model(split['seed'])
    model.fit(train_df[feats], train_df['is_malignant'])
    out = path_for('outputs_dir') / 'model'
    out.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, out / 'model.joblib')
    cfg = {'model_version': f"logreg_seed{split['seed']}", 'family': 'LogisticRegression + StandardScaler',
           'max_iter': SETTINGS['model']['max_iter'], 'seed': split['seed'], 'features': feats,
           'train_rows': len(train_df), 'split_sha256': split['sha256'], 'pipeline_run_id': run_id}
    (out / 'model_config.json').write_text(json.dumps(cfg, indent=2))
    return cfg
