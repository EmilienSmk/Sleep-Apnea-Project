import json
from pymongo import MongoClient
from kafka import KafkaConsumer

def get_patient_profile(patient_id):
    """Fetches the static risk factors from MongoDB."""
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sleep_apnea_db"]
    collection = db["patients"]
    
    profile = collection.find_one({"patient_id": patient_id})
    client.close()
    return profile

def evaluate_apnea_risk(vitals, profile):
    """
    ML/Statistical Anomaly Detection Model.
    Evaluates real-time vitals against personal historical baselines and risk factors.
    """
    spo2 = vitals["spo2"]
    hr = vitals["heart_rate"]
    
    # Base thresholds adjusted dynamically by patient's static profile
    # Higher BMI or prior high risk flags lower the tolerance threshold
    spo2_threshold = profile["baseline_spo2"] - 5
    if profile["bmi"] > 30:
        spo2_threshold += 1 # Strict threshold for higher BMI patients
        
    hr_threshold_high = profile["baseline_hr"] + 20

    # Rule-Based Statistical Anomaly Classification
    is_anomaly = False
    confidence = 0.0
    features_triggered = []

    if spo2 < spo2_threshold:
        is_anomaly = True
        features_triggered.append("SpO2_Drop")
        # Deeper drops indicate higher classification confidence
        confidence += min((spo2_threshold - spo2) * 15, 70.0) 

    if hr > hr_threshold_high:
        is_anomaly = True
        features_triggered.append("HeartRate_Spike")
        confidence += 30.0

    return {
        "is_anomaly": is_anomaly,
        "confidence_score": round(min(confidence, 100.0), 2) if is_anomaly else 0.0,
        "features": features_triggered,
        "status": "APNEA_EVENT_DETECTED" if is_anomaly else "NORMAL"
    }

def start_ml_engine():
    # Allow user to choose a patient to evaluate
    print("--- Available Patients in MongoDB (P001 to P005) ---")
    patient_id = input("Enter Patient ID to monitor (e.g., P001): ").strip().upper()
    
    profile = get_patient_profile(patient_id)
    if not profile:
        print(f"Patient {patient_id} not found in MongoDB. Run generate_patients.py first.")
        return
        
    print(f"\nLoaded Historical Profile for {patient_id}:")
    print(f" Age: {profile['age']} | BMI: {profile['bmi']} | Baseline SpO2: {profile['baseline_spo2']}%")
    print("Listening to real-time vitals stream...")

    # Consume live streaming data from Kafka
    consumer = KafkaConsumer(
        'processed_vitals',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    # Re-publish evaluation results to a new Kafka topic for the visualization dashboard
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for message in consumer:
        vitals = message.value
        
        # Only evaluate data belonging to our selected patient
        if vitals["patient_id"] == patient_id:
            assessment = evaluate_apnea_risk(vitals, profile)
            
            # Combine raw vitals data with the ML prediction output
            output_payload = {
                "timestamp": vitals["timestamp"],
                "patient_id": patient_id,
                "current_spo2": vitals["spo2"],
                "current_hr": vitals["heart_rate"],
                "ml_results": assessment
            }
            
            # Send the predictions down the pipeline to a dedicated topic
            producer.send('ml_predictions', output_payload)
            producer.flush()
            
            # Print feedback to terminal
            status = assessment["status"]
            print(f"[{vitals['timestamp']}] SpO2: {vitals['spo2']}% | HR: {vitals['heart_rate']} bpm -> STATUS: {status} (Conf: {assessment['confidence_score']}%)")

if __name__ == "__main__":
    start_ml_engine()
