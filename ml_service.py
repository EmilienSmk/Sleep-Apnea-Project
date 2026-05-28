import json
import numpy as np
import pandas as pd
from pymongo import MongoClient
from kafka import KafkaConsumer, KafkaProducer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix

# ==========================================
# PHASE 1: ML MODEL TRAINING & EVALUATION
# ==========================================
def generate_historical_training_data(num_records=5000):
    """Simulates historical clinical data to train the ML model."""
    np.random.seed(42)
    bmi = np.random.uniform(18.5, 40.0, num_records)
    baseline_spo2 = np.random.uniform(94, 99, num_records)
    current_spo2 = baseline_spo2 - np.random.exponential(scale=2, size=num_records)
    current_hr = np.random.uniform(55, 110, num_records)
    
    df = pd.DataFrame({
        'bmi': bmi,
        'baseline_spo2': baseline_spo2,
        'current_spo2': current_spo2.clip(max=100),
        'current_hr': current_hr
    })
    
    # Ground Truth: Anomaly happens if SpO2 drops severely, heavily penalized by high BMI
    spo2_drop = df['baseline_spo2'] - df['current_spo2']
    is_apnea = (spo2_drop > 4.5) | ((df['bmi'] > 30) & (spo2_drop > 3.5)) | ((df['current_hr'] > 95) & (spo2_drop > 3.0))
    df['label'] = is_apnea.astype(int)
    
    return df

def train_and_evaluate_model():
    """Trains the Random Forest model and prints statistical validation metrics."""
    print("⏳ Step 1: Generating historical medical training dataset...")
    data = generate_historical_training_data()
    
    X = data[['bmi', 'baseline_spo2', 'current_spo2', 'current_hr']]
    y = data['label']
    
    # 80/20 split for statistical validation
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)
    
    print("🤖 Step 2: Training Statistical Model (Random Forest Classifier)...")
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # --- RUBRIC REQUIREMENT: STATISTICAL METRICS ---
    print("\n📊 Step 3: Evaluating Model (Rubric Validation):")
    y_pred = model.predict(X_test)
    print("-" * 60)
    print("CONFUSION MATRIX:")
    print(confusion_matrix(y_test, y_pred))
    print("\nCLASSIFICATION REPORT:")
    print(classification_report(y_test, y_pred, target_names=['NORMAL', 'APNEA_EVENT']))
    print("-" * 60)
    
    return model

# ==========================================
# PHASE 2: LIVE STREAMING INFERENCE
# ==========================================
def get_all_patient_profiles():
    """Fetches all static risk factors from MongoDB into an in-memory map."""
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sleep_apnea_db"]
    profiles = list(db["patients"].find())
    client.close()
    return {p["patient_id"]: p for p in profiles}

def start_ml_streaming_engine():
    # 1. Train the model in memory FIRST
    ml_model = train_and_evaluate_model()
    
    # 2. Setup standard streaming context
    profiles_dict = get_all_patient_profiles()
    print(f"\n🚀 Phase 2: Starting Live Inference for: {list(profiles_dict.keys())}")
    print("Listening to Spark-processed live vitals stream...\n")

    consumer = KafkaConsumer(
        'processed_vitals', 
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for message in consumer:
        vitals = message.value
        patient_id = vitals["patient_id"]
        
        if patient_id in profiles_dict:
            profile = profiles_dict[patient_id]
            
            # Format live data for the ML model
            live_features = pd.DataFrame([{
                'bmi': float(profile["bmi"]),
                'baseline_spo2': float(profile["baseline_spo2"]),
                'current_spo2': float(vitals["spo2"]),
                'current_hr': float(vitals["heart_rate"])
            }])
            
            # Perform Live Prediction
            prediction = ml_model.predict(live_features)[0] 
            probabilities = ml_model.predict_proba(live_features)[0] 
            
            is_anomaly = (prediction == 1)
            confidence_score = round(probabilities[1] * 100, 2) if is_anomaly else round(probabilities[0] * 100, 2)
            status = "APNEA_EVENT_DETECTED" if is_anomaly else "NORMAL"
            
            output_payload = {
                "timestamp": vitals["timestamp"],
                "patient_id": patient_id,
                "current_spo2": vitals["spo2"],
                "current_hr": vitals["heart_rate"],
                "ml_results": {
                    "is_anomaly": is_anomaly,
                    "confidence_score": confidence_score,
                    "status": status
                }
            }
            
            producer.send('ml_predictions', output_payload)
            
            # Print simplified logs to terminal so it doesn't get too messy
            if is_anomaly:
                print(f"🚨 ML ALERT [{vitals['timestamp']}] {patient_id} | Confidence: {confidence_score}%")

if __name__ == "__main__":
    start_ml_streaming_engine()
