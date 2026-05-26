import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

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
    target_patient = "P004" 
    

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Starting live sensor stream for {target_patient} to Kafka...")
    
    try:
        while True:
            apnea_event = random.random() < 0.10
            data = simulate_sensor(target_patient, apnea_event)
            

            producer.send('sensor_vitals', data)
            producer.flush()
            
            print(f"Streamed to Kafka: {data}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("Simulation stopped.")
