import pandas as pd
import numpy as np
from catboost import CatBoostRegressor, Pool
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib
from datetime import datetime
from sklearn.metrics import r2_score
import os

# Define model output directory and input CSV path
MODEL_DIR = os.getenv("MODEL_DIR", "/opt/airflow/scripts")
CSV_PATH = os.getenv("CSV_PATH", "/opt/airflow/output/combined_requested.csv")
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)

# Load and preprocess data
df = pd.read_csv(CSV_PATH)
df['requested_date'] = pd.to_datetime(df['requested_date'], errors='coerce')
df = df.dropna(subset=['requested_date', 'requested_project_name', 'item_name', 'requested_quantity'])

# Drop unnecessary columns
df = df.drop(columns=[col for col in df.columns if col not in ['item_name', 'requested_project_name', 'requested_date', 'requested_quantity', 'uom_name', 'is_returned', 'current_consumed_amount', 'returned_quantity']])
print("Columns after dropping unnecessary ones:")
print(df.columns)

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
y = df['requested_quantity']

# Split data based on time
split_date = pd.to_datetime('2025-05-01')
train_mask = df['requested_date'] < split_date
X_train, y_train = X[train_mask], y[train_mask]
X_test, y_test = X[~train_mask], y[~train_mask]
print(f"Train size: {len(X_train)}, Test size: {len(X_test)}")

# Train CatBoost model
cat_model = CatBoostRegressor(
    iterations=500,
    learning_rate=0.1,
    depth=6,
    eval_metric='MAE',
    random_seed=42,
    verbose=100
)
cat_features = ['project_enc', 'item_enc']
train_pool = Pool(X_train, y_train, cat_features=cat_features)
test_pool = Pool(X_test, y_test, cat_features=cat_features)
cat_model.fit(train_pool, eval_set=test_pool, early_stopping_rounds=50)

# Predict and evaluate
y_pred = cat_model.predict(X_test)
print("\nCatBoost Evaluation (Time-based Split):")
print(f"MAE: {mean_absolute_error(y_test, y_pred):.2f}")
print(f"RMSE: {np.sqrt(mean_squared_error(y_test, y_pred)):.2f}")
print(f"R²: {r2_score(y_test, y_pred):.2f}")

# Save model and encoders
joblib.dump(cat_model, os.path.join(MODEL_DIR, 'catboost_model.pkl'))
joblib.dump(le_project, os.path.join(MODEL_DIR, 'le_project.pkl'))
joblib.dump(le_item, os.path.join(MODEL_DIR, 'le_item.pkl'))
print(f"Model and encoders saved to {MODEL_DIR}: 'catboost_model.pkl', 'le_project.pkl', 'le_item.pkl'")