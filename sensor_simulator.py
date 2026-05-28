import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from pymongo import MongoClient

def get_active_patients():
    """Fetch all generated patient IDs directly from MongoDB."""
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sleep_apnea_db"]
    patients = db["patients"].find({}, {"patient_id": 1})
    patient_ids = [p["patient_id"] for p in patients]
    client.close()
    return patient_ids

def simulate_sensor(patient_id, is_apnea_event=False):
    spo2 = random.randint(95, 99)
    hr = random.randint(60, 85)

    if is_apnea_event:
        spo2 = random.randint(80, 89)
        hr = random.randint(90, 110)

    payload = {
        "timestamp": datetime.now().isoformat(),
        "patient_id": patient_id,
        "spo2": spo2,
        "heart_rate": hr
    }
    return payload

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    patient_ids = get_active_patients()
    if not patient_ids:
        print("No patients found in MongoDB. Please run generate_patients.py first.")
        exit()
        
    print(f"Starting simultaneous live sensor streams for {len(patient_ids)} patients...")
    print(f"Active Roster: {patient_ids}")
    
    try:
        while True:
            # Emit a sensor reading for EVERY patient in the database
            for patient_id in patient_ids:
                # 10% chance per patient to simulate an acute event
                apnea_event = random.random() < 0.10
                data = simulate_sensor(patient_id, apnea_event)
                
                producer.send('sensor_vitals', data)
                print(f"Streamed: {data}")
            
            producer.flush()
            time.sleep(2) # Wait 2 seconds before the next batch of readings
            
    except KeyboardInterrupt:
        print("\nSimulation stopped.")
