# # Define model output directory and input CSV path
# MODEL_DIR = os.getenv("MODEL_DIR", "/opt/airflow/scripts")
# CSV_PATH = os.getenv("CSV_PATH", "/opt/airflow/output/combined_requested.csv")
# os.makedirs(MODEL_DIR, exist_ok=True)
# os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

import pandas as pd
import numpy as np
from catboost import CatBoostRegressor, Pool
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
from datetime import datetime
import os
import json

# -------------------------
# Config (change if needed)
# -------------------------
SCRIPTS_DIR = os.path.join(os.getcwd(), "scripts")  # root/scripts

# Path to save models (inside scripts/)
MODEL_DIR = os.path.join(os.getcwd())  # current scripts folder

# Correct CSV path relative to scripts folder
CSV_PATH = os.path.join(os.path.dirname(os.getcwd()), "airflow", "output", "combined_requested1.csv")

os.makedirs(SCRIPTS_DIR, exist_ok=True)
os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

# -------------------------
# Load and preprocess data
# -------------------------
df = pd.read_csv(CSV_PATH)
df['requested_date'] = pd.to_datetime(df['requested_date'], errors='coerce')
df = df.dropna(subset=['requested_date', 'requested_project_name', 'item_name', 'requested_quantity'])

# Keep only required columns
keep_cols = ['item_name', 'requested_project_name', 'requested_date', 'requested_quantity',
             'uom_name', 'is_returned', 'current_consumed_amount', 'returned_quantity']
df = df[[c for c in df.columns if c in keep_cols]]
print("Columns after dropping unnecessary ones:")
print(df.columns.tolist())

# Feature engineering
df['month'] = df['requested_date'].dt.month
df['year'] = df['requested_date'].dt.year
df['day'] = df['requested_date'].dt.day
df['in_use'] = ((df['is_returned'] == 0) & (df['current_consumed_amount'] == 0)).astype(int)

# Encode categorical variables
le_project = LabelEncoder()
le_item = LabelEncoder()
df['project_enc'] = le_project.fit_transform(df['requested_project_name'])
df['item_enc'] = le_item.fit_transform(df['item_name'])

# Prepare features and target
features = ['project_enc', 'item_enc', 'month', 'year', 'day', 'in_use']
X = df[features]
y = df['requested_quantity'].astype(float)  # keep numeric dtype

# Split data based on time (time-based split)
split_date = pd.to_datetime('2025-05-01')
train_mask = df['requested_date'] < split_date
X_train, y_train = X[train_mask], y[train_mask]
X_test, y_test = X[~train_mask], y[~train_mask]
print(f"Train size: {len(X_train)}, Test size: {len(X_test)}")

# If test set is empty, warn and exit gracefully
if len(X_test) == 0:
    print("Warning: Test set is empty after the time split. Adjust 'split_date' or check your data.")
    # still proceed to train on all data if you want; here we stop to avoid silent failures
    raise SystemExit(1)

# -------------------------
# Target transform: log1p
# -------------------------
# Recommended for skewed count-like targets.
y_train_log = np.log1p(y_train.values)   # transform training target
y_test_log  = np.log1p(y_test.values)    # used for CatBoost eval_set

# -------------------------
# Train CatBoost model
# -------------------------
cat_model = CatBoostRegressor(
    iterations=500,
    learning_rate=0.1,
    depth=6,
    eval_metric='MAE',
    random_seed=42,
    verbose=100
)

cat_features = ['project_enc', 'item_enc']

# Build Pool objects using the transformed target
train_pool = Pool(X_train, y_train_log, cat_features=cat_features)
test_pool = Pool(X_test, y_test_log, cat_features=cat_features)

cat_model.fit(train_pool, eval_set=test_pool, early_stopping_rounds=50)

# -------------------------
# Predict -> inverse transform -> integer outputs
# -------------------------
# Predictions are on log1p scale
y_pred_log = cat_model.predict(X_test)

# Inverse transform (expm1), round to nearest integer, clip negatives
y_pred = np.expm1(y_pred_log)             # inverse of log1p
y_pred = np.rint(y_pred).astype(int)      # round to nearest integer
y_pred = np.clip(y_pred, 0, None)         # ensure non-negative

# Evaluate on original scale
print("\nCatBoost Evaluation (Time-based Split, log1p target):")
print(f"MAE: {mean_absolute_error(y_test, y_pred):.2f}")
print(f"RMSE: {np.sqrt(mean_squared_error(y_test, y_pred)):.2f}")
print(f"R²: {r2_score(y_test, y_pred):.2f}")

# -------------------------
# Save model, encoders, metadata
# -------------------------
try:
    joblib.dump(cat_model, os.path.join(MODEL_DIR, 'catboost_model.pkl'))
    joblib.dump(le_project, os.path.join(MODEL_DIR, 'le_project.pkl'))
    joblib.dump(le_item, os.path.join(MODEL_DIR, 'le_item.pkl'))

    # Save label encoder classes mapping as JSON for easy inspection in production
    proj_map = {int(i): str(c) for i, c in enumerate(le_project.classes_)}
    item_map = {int(i): str(c) for i, c in enumerate(le_item.classes_)}
    with open(os.path.join(MODEL_DIR, 'label_mappings.json'), 'w', encoding='utf-8') as f:
        json.dump({'project_enc_map': proj_map, 'item_enc_map': item_map}, f, ensure_ascii=False, indent=2)

    # Metadata describing preprocessing & expected inference steps
    metadata = {
        "created_at": datetime.utcnow().isoformat() + "Z",
        "target_transform": "log1p",
        "inverse_transform": "expm1",
        "prediction_postprocessing": "round_to_nearest_int_then_clip_min_0",
        "features": features,
        "cat_features": cat_features,
        "notes": "During inference apply model.predict(X_raw_transformed_features) -> np.expm1(pred) -> round -> clip >=0"
    }
    with open(os.path.join(MODEL_DIR, 'model_metadata.json'), 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"Model, encoders, label mappings, and metadata saved to {MODEL_DIR}")
except Exception as e:
    print("Warning: failed to save one or more artifacts. Error:", e)