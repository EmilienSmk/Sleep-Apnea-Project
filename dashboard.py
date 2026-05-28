import streamlit as st
import json
import pandas as pd
from pymongo import MongoClient
from kafka import KafkaConsumer
import time
import altair as alt


st.set_page_config(page_title="Sleep Apnea Monitor", layout="wide")
st.title("Real-Time Sleep Apnea Detection Dashboard")

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


selected_patient_id = st.sidebar.selectbox("Select Patient to Monitor", patient_ids)


if "anomaly_log" not in st.session_state:
    st.session_state.anomaly_log = pd.DataFrame(
        columns=['Timestamp', 'Patient ID', 'SpO2', 'Heart Rate', 'Status', 'Confidence']
    )


if "current_patient" not in st.session_state or st.session_state.current_patient != selected_patient_id:
    st.session_state.current_patient = selected_patient_id

    st.session_state.history = pd.DataFrame(columns=['timestamp', 'spo2', 'heart_rate', 'status'])
    st.session_state.current_spo2 = None
    st.session_state.current_hr = None
    st.session_state.ml_status = "Waiting for data stream..."
    st.session_state.confidence_score = 0.0


selected_profile = next(p for p in patients if p["patient_id"] == selected_patient_id)
st.sidebar.subheader("Patient Static Profile")
st.sidebar.write(f"**Age:** {selected_profile['age']}")
st.sidebar.write(f"**BMI:** {selected_profile['bmi']}")
st.sidebar.write(f"**Baseline SpO2:** {selected_profile['baseline_spo2']}%")
st.sidebar.write(f"**Baseline HR:** {selected_profile['baseline_hr']} bpm")
st.sidebar.write(f"**Historical Apnea Risk:** {'High' if selected_profile['apnea_risk'] else 'Low'}")


st.subheader(f"Live Telemetry & ML Predictions for {selected_patient_id}")
col1, col2, col3 = st.columns(3)


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


if "ALERT" in st.session_state.ml_status or "DETECTED" in st.session_state.ml_status:
    col3.error(f" **{st.session_state.ml_status}**\n\nConfidence: {st.session_state.confidence_score}%")
elif "NORMAL" in st.session_state.ml_status:
    col3.success(f" **{st.session_state.ml_status}**")
else:
    col3.info(f" {st.session_state.ml_status}")


st.subheader("Vitals Real-Time Monitoring Graph")
if not st.session_state.history.empty:
    chart_data = st.session_state.history.copy()
    chart_data['timestamp'] = pd.to_datetime(chart_data['timestamp'])
    

    melted_data = chart_data.melt(
        id_vars=['timestamp', 'status'], 
        value_vars=['spo2', 'heart_rate'], 
        var_name='Metric', 
        value_name='Value'
    )


    base_lines = alt.Chart(melted_data).mark_line().encode(
        x=alt.X('timestamp:T', title='Time'),
        y=alt.Y('Value:Q', title='Vitals', scale=alt.Scale(zero=False)),
        color=alt.Color('Metric:N', scale=alt.Scale(domain=['spo2', 'heart_rate'], range=['#1f77b4', '#2ca02c']))
    )


    anomaly_data = melted_data[melted_data['status'].str.contains("ALERT|DETECTED", case=False, na=False)]
    

    anomaly_dots = alt.Chart(anomaly_data).mark_circle(color='red', size=120, opacity=1).encode(
        x='timestamp:T',
        y='Value:Q',
        tooltip=['timestamp:T', 'Metric:N', 'Value:Q', 'status:N'] 
    )


    final_chart = alt.layer(base_lines, anomaly_dots).interactive()
    st.altair_chart(final_chart, width='stretch')
else:
    st.info("Graph will automatically populate as data arrives from Apache Spark & ML Service...")


st.markdown("---")
st.subheader(" Historical Anomaly Log (All Patients)")

if not st.session_state.anomaly_log.empty:
    st.dataframe(
        st.session_state.anomaly_log, 
        width="stretch", 
        hide_index=True
    )
else:
    st.info("No sleep apnea anomalies detected in this session yet.")


@st.cache_resource
def get_kafka_consumer():
    return KafkaConsumer(
        'ml_predictions',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

consumer = get_kafka_consumer()


msg_pack = consumer.poll(timeout_ms=100)

for tp, messages in msg_pack.items():
    for message in messages:
        data = message.value
        status = data["ml_results"]["status"]
        timestamp_str = pd.to_datetime(data["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')


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


        if data["patient_id"] == selected_patient_id:
            st.session_state.current_spo2 = data["current_spo2"]
            st.session_state.current_hr = data["current_hr"]
            st.session_state.ml_status = status
            st.session_state.confidence_score = data["ml_results"]["confidence_score"]
            

            new_row = pd.DataFrame([{
                'timestamp': data["timestamp"],
                'spo2': data["current_spo2"],
                'heart_rate': data["current_hr"],
                'status': status  
            }])
            st.session_state.history = pd.concat([st.session_state.history, new_row]).tail(50)


time.sleep(1)
st.rerun()
