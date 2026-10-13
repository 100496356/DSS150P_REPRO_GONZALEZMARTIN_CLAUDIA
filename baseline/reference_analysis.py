"""Baseline "previous project": a notebook-style analysis kept exactly as a typical class notebook.

It reads the CSV from a hard-coded relative path, splits once, trains a logistic regression and
prints the metrics. No checksum, no layers, no logging, no database. The engineered pipeline in
src/ reproduces this result; metadata/expected_metrics.json stores the numbers printed here.
"""
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

df = pd.read_csv('data/source/breast_cancer_wisconsin.csv')
X = df.drop(columns=['record_id', 'diagnosis'])
y = (df['diagnosis'] == 'M').astype(int)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=5000))
model.fit(X_train, y_train)
pred = model.predict(X_test)
proba = model.predict_proba(X_test)[:, 1]
print('accuracy', round(accuracy_score(y_test, pred), 4))
print('f1', round(f1_score(y_test, pred), 4))
print('roc_auc', round(roc_auc_score(y_test, proba), 4))
