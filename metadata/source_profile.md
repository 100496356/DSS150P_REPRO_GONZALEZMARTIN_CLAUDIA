# Source profile: Breast Cancer Wisconsin (Diagnostic)

Generated from `data/source/breast_cancer_wisconsin.csv` (the frozen source snapshot).

| Property | Value |
|---|---|
| File / format | `data/source/breast_cancer_wisconsin.csv` · CSV, UTF-8, comma-delimited, header row |
| Rows × columns | 569 × 32 |
| File size | 123,566 bytes |
| SHA-256 | `e4fcbf92dc89d0bda0d8610d6ccba7b8b1e38382a704ce5e7be47a6c93fec24c` |
| Origin | UCI Machine Learning Repository (Wolberg, Street, Mangasarian, 1995), https://doi.org/10.24432/C5DW2B |
| Copy used | scikit-learn 1.5.2 sklearn.datasets.load_breast_cancer (exported by scripts/export_source.py) |
| License / privacy | CC BY 4.0 (redistribution allowed with attribution) · de-identified numeric features computed from images; no names or personal identifiers |
| Update frequency | Static snapshot; no natural update timestamp |

## Keys, duplicates and target

- Candidate key: `record_id` (1..569, unique: True). It is a stable sequence assigned from the immutable source order; the original UCI ID is not included in the scikit-learn copy.
- Exact duplicate rows: 0. Duplicate feature rows ignoring `record_id`: 0. Duplicate `record_id`: 0.
- Target `diagnosis`: B = 357 (62.7%), M = 212 (37.3%). Moderate imbalance, so the split is stratified and F1/ROC-AUC are tracked with accuracy.
- Leakage risks: `record_id` is an identifier and is excluded from features; no field is derived from the diagnosis. No timestamps.
- Schema risks: 30 float columns with similar names (`mean_*`, `*_error`, `worst_*`); a renamed or dropped column would silently change the feature matrix, so the contract fixes names and order.

## Columns

| Column | Type | Nulls | Distinct | Min | Max |
|---|---|---|---|---|---|
| record_id | integer | 0 | 569 | 1 | 569 |
| mean_radius | float | 0 | 456 | 6.981 | 28.11 |
| mean_texture | float | 0 | 479 | 9.71 | 39.28 |
| mean_perimeter | float | 0 | 522 | 43.79 | 188.5 |
| mean_area | float | 0 | 539 | 143.5 | 2501 |
| mean_smoothness | float | 0 | 474 | 0.05263 | 0.1634 |
| mean_compactness | float | 0 | 537 | 0.01938 | 0.3454 |
| mean_concavity | float | 0 | 537 | 0 | 0.4268 |
| mean_concave_points | float | 0 | 542 | 0 | 0.2012 |
| mean_symmetry | float | 0 | 432 | 0.106 | 0.304 |
| mean_fractal_dimension | float | 0 | 499 | 0.04996 | 0.09744 |
| radius_error | float | 0 | 540 | 0.1115 | 2.873 |
| texture_error | float | 0 | 519 | 0.3602 | 4.885 |
| perimeter_error | float | 0 | 533 | 0.757 | 21.98 |
| area_error | float | 0 | 528 | 6.802 | 542.2 |
| smoothness_error | float | 0 | 547 | 0.001713 | 0.03113 |
| compactness_error | float | 0 | 541 | 0.002252 | 0.1354 |
| concavity_error | float | 0 | 533 | 0 | 0.396 |
| concave_points_error | float | 0 | 507 | 0 | 0.05279 |
| symmetry_error | float | 0 | 498 | 0.007882 | 0.07895 |
| fractal_dimension_error | float | 0 | 545 | 0.0008948 | 0.02984 |
| worst_radius | float | 0 | 457 | 7.93 | 36.04 |
| worst_texture | float | 0 | 511 | 12.02 | 49.54 |
| worst_perimeter | float | 0 | 514 | 50.41 | 251.2 |
| worst_area | float | 0 | 544 | 185.2 | 4254 |
| worst_smoothness | float | 0 | 411 | 0.07117 | 0.2226 |
| worst_compactness | float | 0 | 529 | 0.02729 | 1.058 |
| worst_concavity | float | 0 | 539 | 0 | 1.252 |
| worst_concave_points | float | 0 | 492 | 0 | 0.291 |
| worst_symmetry | float | 0 | 500 | 0.1565 | 0.6638 |
| worst_fractal_dimension | float | 0 | 535 | 0.05504 | 0.2075 |
| diagnosis | string (M/B) | 0 | 2 | - | - |
