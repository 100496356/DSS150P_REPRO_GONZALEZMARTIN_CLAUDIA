-- Target schema. Applied by: python -m src.cli init-db (idempotent).
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS curated.breast_cancer_features (
  record_id INTEGER PRIMARY KEY,
  mean_radius DOUBLE PRECISION NOT NULL CHECK (mean_radius >= 0),
  mean_texture DOUBLE PRECISION NOT NULL CHECK (mean_texture >= 0),
  mean_perimeter DOUBLE PRECISION NOT NULL CHECK (mean_perimeter >= 0),
  mean_area DOUBLE PRECISION NOT NULL CHECK (mean_area >= 0),
  mean_smoothness DOUBLE PRECISION NOT NULL CHECK (mean_smoothness >= 0),
  mean_compactness DOUBLE PRECISION NOT NULL CHECK (mean_compactness >= 0),
  mean_concavity DOUBLE PRECISION NOT NULL CHECK (mean_concavity >= 0),
  mean_concave_points DOUBLE PRECISION NOT NULL CHECK (mean_concave_points >= 0),
  mean_symmetry DOUBLE PRECISION NOT NULL CHECK (mean_symmetry >= 0),
  mean_fractal_dimension DOUBLE PRECISION NOT NULL CHECK (mean_fractal_dimension >= 0),
  radius_error DOUBLE PRECISION NOT NULL CHECK (radius_error >= 0),
  texture_error DOUBLE PRECISION NOT NULL CHECK (texture_error >= 0),
  perimeter_error DOUBLE PRECISION NOT NULL CHECK (perimeter_error >= 0),
  area_error DOUBLE PRECISION NOT NULL CHECK (area_error >= 0),
  smoothness_error DOUBLE PRECISION NOT NULL CHECK (smoothness_error >= 0),
  compactness_error DOUBLE PRECISION NOT NULL CHECK (compactness_error >= 0),
  concavity_error DOUBLE PRECISION NOT NULL CHECK (concavity_error >= 0),
  concave_points_error DOUBLE PRECISION NOT NULL CHECK (concave_points_error >= 0),
  symmetry_error DOUBLE PRECISION NOT NULL CHECK (symmetry_error >= 0),
  fractal_dimension_error DOUBLE PRECISION NOT NULL CHECK (fractal_dimension_error >= 0),
  worst_radius DOUBLE PRECISION NOT NULL CHECK (worst_radius >= 0),
  worst_texture DOUBLE PRECISION NOT NULL CHECK (worst_texture >= 0),
  worst_perimeter DOUBLE PRECISION NOT NULL CHECK (worst_perimeter >= 0),
  worst_area DOUBLE PRECISION NOT NULL CHECK (worst_area >= 0),
  worst_smoothness DOUBLE PRECISION NOT NULL CHECK (worst_smoothness >= 0),
  worst_compactness DOUBLE PRECISION NOT NULL CHECK (worst_compactness >= 0),
  worst_concavity DOUBLE PRECISION NOT NULL CHECK (worst_concavity >= 0),
  worst_concave_points DOUBLE PRECISION NOT NULL CHECK (worst_concave_points >= 0),
  worst_symmetry DOUBLE PRECISION NOT NULL CHECK (worst_symmetry >= 0),
  worst_fractal_dimension DOUBLE PRECISION NOT NULL CHECK (worst_fractal_dimension >= 0),
  diagnosis CHAR(1) NOT NULL CHECK (diagnosis IN ('M','B')),
  is_malignant SMALLINT NOT NULL CHECK (is_malignant IN (0,1)),
  split TEXT NOT NULL CHECK (split IN ('train','test')),
  record_hash TEXT NOT NULL,
  pipeline_run_id TEXT NOT NULL,
  processed_at_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS curated.predictions (
  record_id INTEGER NOT NULL REFERENCES curated.breast_cancer_features(record_id),
  model_version TEXT NOT NULL,
  y_true SMALLINT NOT NULL,
  y_pred SMALLINT NOT NULL,
  proba_malignant DOUBLE PRECISION NOT NULL,
  pipeline_run_id TEXT NOT NULL,
  predicted_at_utc TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (record_id, model_version)
);

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
  pipeline_run_id TEXT PRIMARY KEY,
  started_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at_utc TIMESTAMPTZ,
  status TEXT NOT NULL,
  raw_rows INTEGER, staging_rows INTEGER, curated_rows INTEGER,
  train_rows INTEGER, test_rows INTEGER, prediction_rows INTEGER,
  accuracy DOUBLE PRECISION, reproducibility_passed BOOLEAN,
  message TEXT
);

CREATE TABLE IF NOT EXISTS audit.partition_loads (
  partition_key TEXT PRIMARY KEY,
  row_count INTEGER NOT NULL,
  pipeline_run_id TEXT NOT NULL,
  loaded_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);
