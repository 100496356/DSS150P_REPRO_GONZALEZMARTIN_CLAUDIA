"""How data/source/breast_cancer_wisconsin.csv was produced (run once, kept for provenance).

The Breast Cancer Wisconsin (Diagnostic) dataset (UCI, CC BY 4.0) is bundled with scikit-learn.
This script exports it unchanged to CSV, adds a stable record_id (immutable source order) and
writes the diagnosis as M/B like the original UCI file. The pipeline never runs this script:
the CSV in data/source/ is the frozen source snapshot, verified by its SHA-256 checksum.
"""
import re

import pandas as pd
from sklearn.datasets import load_breast_cancer

d = load_breast_cancer(as_frame=True)
df = d.frame.copy()
df.columns = [re.sub(r'[^a-z0-9]+', '_', c.lower()).strip('_') for c in df.columns]
df['diagnosis'] = df.pop('target').map({0: 'M', 1: 'B'})
df.insert(0, 'record_id', range(1, len(df) + 1))
df.to_csv('data/source/breast_cancer_wisconsin.csv', index=False)
