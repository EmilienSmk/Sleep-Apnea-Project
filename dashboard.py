import streamlit as st
import json
import pandas as pd
from pymongo import MongoClient
from kafka import KafkaConsumer
import time
import altair as alt

# Set up the page layout
st.set_page_config(page_title="Sleep Apnea Monitor", layout="wide")
st.title("Real-Time Sleep Apnea Detection Dashboard")

# 1. Connect to MongoDB to fetch static patient profiles
@st.cache_resource
def get_mongo_client():
    return MongoClient("mongodb://localhost:27017/")

client = get_mongo_client()
db = client["sleep_apnea_db"]
collection = db["patients"]

patients = list(collection.find({}, {"_id": 0}))
patient_ids = [p["patient_id"] for p in patients]

if not patient_ids:
    st.error("No patients found in MongoDB. Run generate_patients.py first.")
    st.stop()

# UI Control: Select Box to change patients dynamically
selected_patient_id = st.sidebar.selectbox("Select Patient to Monitor", patient_ids)

# 2. State Management: Initialize persistent variables in st.session_state
if "anomaly_log" not in st.session_state:
    st.session_state.anomaly_log = pd.DataFrame(
        columns=['Timestamp', 'Patient ID', 'SpO2', 'Heart Rate', 'Status', 'Confidence']
    )

# Reset patient-specific metrics when switching drop-downs
if "current_patient" not in st.session_state or st.session_state.current_patient != selected_patient_id:
    st.session_state.current_patient = selected_patient_id
    # Added 'status' so the graph knows when to draw the red dots
    st.session_state.history = pd.DataFrame(columns=['timestamp', 'spo2', 'heart_rate', 'status'])
    st.session_state.current_spo2 = None
    st.session_state.current_hr = None
    st.session_state.ml_status = "Waiting for data stream..."
    st.session_state.confidence_score = 0.0

# 3. Display the Static Profile for the selected patient
selected_profile = next(p for p in patients if p["patient_id"] == selected_patient_id)
st.sidebar.subheader("Patient Static Profile")
st.sidebar.write(f"**Age:** {selected_profile['age']}")
st.sidebar.write(f"**BMI:** {selected_profile['bmi']}")
st.sidebar.write(f"**Baseline SpO2:** {selected_profile['baseline_spo2']}%")
st.sidebar.write(f"**Baseline HR:** {selected_profile['baseline_hr']} bpm")
st.sidebar.write(f"**Historical Apnea Risk:** {'High' if selected_profile['apnea_risk'] else 'Low'}")

# 4. Lay out the live UI elements
st.subheader(f"Live Telemetry & ML Predictions for {selected_patient_id}")
col1, col2, col3 = st.columns(3)

# Render metrics from Session State
if st.session_state.current_spo2 is not None:
    spo2_delta = st.session_state.current_spo2 - selected_profile['baseline_spo2']
    col1.metric("Current SpO2", f"{st.session_state.current_spo2}%", delta=f"{spo2_delta}%")
else:
    col1.metric("Current SpO2", "Connecting...")

if st.session_state.current_hr is not None:
    hr_delta = st.session_state.current_hr - selected_profile['baseline_hr']
    col2.metric("Current Heart Rate", f"{st.session_state.current_hr} bpm", delta=f"{hr_delta} bpm", delta_color="inverse")
else:
    col2.metric("Current Heart Rate", "Connecting...")

# Display ML Status Notification based on algorithm predictions
if "ALERT" in st.session_state.ml_status or "DETECTED" in st.session_state.ml_status:
    col3.error(f"🚨 **{st.session_state.ml_status}**\n\nConfidence: {st.session_state.confidence_score}%")
elif "NORMAL" in st.session_state.ml_status:
    col3.success(f"✅ **{st.session_state.ml_status}**")
else:
    col3.info(f"ℹ️ {st.session_state.ml_status}")

# 5. Render the Live Timeline Graph with Anomaly Indicators (Red Dots)
st.subheader("Vitals Real-Time Monitoring Graph")
if not st.session_state.history.empty:
    chart_data = st.session_state.history.copy()
    chart_data['timestamp'] = pd.to_datetime(chart_data['timestamp'])
    
    # Restructure dataframe so Altair can plot both lines easily
    melted_data = chart_data.melt(
        id_vars=['timestamp', 'status'], 
        value_vars=['spo2', 'heart_rate'], 
        var_name='Metric', 
        value_name='Value'
    )

    # Draw the continuous base lines for SpO2 and Heart Rate
    base_lines = alt.Chart(melted_data).mark_line().encode(
        x=alt.X('timestamp:T', title='Time'),
        y=alt.Y('Value:Q', title='Vitals', scale=alt.Scale(zero=False)), # Prevents graph from squishing to zero
        color=alt.Color('Metric:N', scale=alt.Scale(domain=['spo2', 'heart_rate'], range=['#1f77b4', '#2ca02c']))
    )

    # Filter out normal data, keeping only the anomalies
    anomaly_data = melted_data[melted_data['status'].str.contains("ALERT|DETECTED", case=False, na=False)]
    
    # Draw bright red dots on the exact timestamps and values where an anomaly occurred
    anomaly_dots = alt.Chart(anomaly_data).mark_circle(color='red', size=120, opacity=1).encode(
        x='timestamp:T',
        y='Value:Q',
        tooltip=['timestamp:T', 'Metric:N', 'Value:Q', 'status:N'] # Hovering shows details!
    )

    # Layer the dots directly over the lines
    final_chart = alt.layer(base_lines, anomaly_dots).interactive()
    st.altair_chart(final_chart, width='stretch')
else:
    st.info("Graph will automatically populate as data arrives from Apache Spark & ML Service...")

# 6. Persistent Anomaly Log Audit Trail
st.markdown("---")
st.subheader("📋 Historical Anomaly Log (All Patients)")

if not st.session_state.anomaly_log.empty:
    st.dataframe(
        st.session_state.anomaly_log, 
        width="stretch", 
        hide_index=True
    )
else:
    st.info("No sleep apnea anomalies detected in this session yet.")

# 7. Cache Kafka connection to keep it open persistently across script reruns
@st.cache_resource
def get_kafka_consumer():
    return KafkaConsumer(
        'ml_predictions',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

consumer = get_kafka_consumer()

# 8. Ingest pending messages using .poll() to prevent generator collisions
msg_pack = consumer.poll(timeout_ms=100)

for tp, messages in msg_pack.items():
    for message in messages:
        data = message.value
        status = data["ml_results"]["status"]
        timestamp_str = pd.to_datetime(data["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')

        # Global Anomaly Checker
        if "ALERT" in status or "DETECTED" in status:
            is_duplicate = not st.session_state.anomaly_log[
                (st.session_state.anomaly_log['Timestamp'] == timestamp_str) & 
                (st.session_state.anomaly_log['Patient ID'] == data["patient_id"])
            ].empty
            
            if not is_duplicate:
                new_anomaly = pd.DataFrame([{
                    'Timestamp': timestamp_str,
                    'Patient ID': data["patient_id"],
                    'SpO2': f"{data['current_spo2']}%",
                    'Heart Rate': f"{data['current_hr']} bpm",
                    'Status': status.replace('_', ' '),
                    'Confidence': f"{data['ml_results']['confidence_score']}%"
                }])
                st.session_state.anomaly_log = pd.concat(
                    [new_anomaly, st.session_state.anomaly_log], 
                    ignore_index=True
                )

        # Maintain real-time layout filter for the selected UI patient
        if data["patient_id"] == selected_patient_id:
            st.session_state.current_spo2 = data["current_spo2"]
            st.session_state.current_hr = data["current_hr"]
            st.session_state.ml_status = status
            st.session_state.confidence_score = data["ml_results"]["confidence_score"]
            
            # Append data to rolling tracking dataframe (NOW INCLUDES STATUS)
            new_row = pd.DataFrame([{
                'timestamp': data["timestamp"],
                'spo2': data["current_spo2"],
                'heart_rate': data["current_hr"],
                'status': status  # Crucial for the red dots to render!
            }])
            st.session_state.history = pd.concat([st.session_state.history, new_row]).tail(50)

# Continuous render control loop
time.sleep(1)
st.rerun()
