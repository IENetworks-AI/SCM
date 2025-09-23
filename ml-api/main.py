from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import pandas as pd
from datetime import datetime
import os

app = FastAPI(title="SCM ML API (Local Testing)")

# Model and encoder paths
MODEL_DIR = os.getenv("MODEL_DIR", "/app/scripts")
MODEL_PATH = os.path.join(MODEL_DIR, 'catboost_model.pkl')
PROJECT_ENCODER_PATH = os.path.join(MODEL_DIR, 'le_project.pkl')
ITEM_ENCODER_PATH = os.path.join(MODEL_DIR, 'le_item.pkl')

# Load model and encoders
try:
    model = joblib.load(MODEL_PATH)
    le_project = joblib.load(PROJECT_ENCODER_PATH)
    le_item = joblib.load(ITEM_ENCODER_PATH)
except FileNotFoundError:
    model = None
    le_project = None
    le_item = None

# Pydantic model for prediction input
class PredictionInput(BaseModel):
    project_name: str
    item_name: str
    requested_date: str  # ISO format: YYYY-MM-DD
    in_use: int  # 0 or 1

@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.post("/predict")
def predict(input: PredictionInput):
    """Predict requested_quantity using CatBoost model."""
    if not model or not le_project or not le_item:
        raise HTTPException(status_code=404, detail="Model or encoders not found")
    
    try:
        # Parse date
        date = pd.to_datetime(input.requested_date)
        month = date.month
        year = date.year
        day = date.day
        
        # Encode inputs
        project_enc = le_project.transform([input.project_name])[0] if input.project_name in le_project.classes_ else -1
        item_enc = le_item.transform([input.item_name])[0] if input.item_name in le_item.classes_ else -1
        
        if project_enc == -1 or item_enc == -1:
            raise HTTPException(status_code=400, detail="Unknown project or item name")
        
        # Prepare features
        features = pd.DataFrame({
            'project_enc': [project_enc],
            'item_enc': [item_enc],
            'month': [month],
            'year': [year],
            'day': [day],
            'in_use': [input.in_use]
        })
        
        # Predict
        prediction = model.predict(features)[0]
        return {"predicted_quantity": float(prediction)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")

@app.post("/retrain")
def trigger_retrain():
    """Trigger retraining of the CatBoost model."""
    try:
        # For local testing, execute train_model.py directly
        exec(open("/app/scripts/train_model.py").read())
        return {"status": "Retraining completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Retraining error: {str(e)}")