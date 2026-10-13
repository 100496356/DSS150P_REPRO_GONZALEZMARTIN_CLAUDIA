-- Serving views on top of the curated tables (applied by init-db, safe to re-run).
CREATE OR REPLACE VIEW curated.v_class_balance AS
SELECT split, diagnosis, count(*) AS rows
FROM curated.breast_cancer_features
GROUP BY split, diagnosis;

CREATE OR REPLACE VIEW curated.v_prediction_quality AS
SELECT model_version,
       count(*) AS test_rows,
       avg((y_true = y_pred)::int)::numeric(6,4) AS accuracy,
       sum(CASE WHEN y_true = 1 AND y_pred = 0 THEN 1 ELSE 0 END) AS false_negatives,
       sum(CASE WHEN y_true = 0 AND y_pred = 1 THEN 1 ELSE 0 END) AS false_positives
FROM curated.predictions
GROUP BY model_version;
