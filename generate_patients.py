import random
from pymongo import MongoClient

def generate_patients(num_patients=5):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["sleep_apnea_db"]
    collection = db["patients"]
    
    collection.delete_many({})

    patients = []
    for i in range(1, num_patients + 1):
        patient = {
            "patient_id": f"P{i:03d}",
            "age": random.randint(30, 80),
            "bmi": round(random.uniform(22.0, 40.0), 1),
            "baseline_spo2": random.randint(95, 99),
            "baseline_hr": random.randint(60, 90),
            "apnea_risk": random.choice([True, False])
        }
        patients.append(patient)
    
    collection.insert_many(patients)
    print(f"Inserted {num_patients} patient profiles into MongoDB.")

if __name__ == "__main__":
    generate_patients()
