import time
import json
import fastf1
import pandas as pd
from kafka import KafkaProducer
import os

# Enable FastF1 cache
current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
fastf1.Cache.enable_cache(os.path.join(current_dir, 'f1_cache'))

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'telemetry-data'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

def get_lap_data():
    # Use a more recent season as 2025 data isn't available yet
    session = fastf1.get_session(2023, 'Chinese Grand Prix', 'R')
    session.load()
    laps = session.laps

    # Convert time-related columns to float seconds or string
    time_cols = [
        'LapTime', 'PitOutTime', 'PitInTime',
        'Sector1Time', 'Sector2Time', 'Sector3Time',
        'Sector1SessionTime', 'Sector2SessionTime', 'Sector3SessionTime',
        'LapStartTime'
    ]
    timestamp_cols = ['LapStartDate']

    for col in time_cols:
        if col in laps.columns:
            laps[col] = laps[col].apply(lambda x: x.total_seconds() if pd.notnull(x) else None)

    for col in timestamp_cols:
        if col in laps.columns:
            laps[col] = laps[col].astype(str)

    # Convert full DataFrame to dicts
    return laps.fillna("").to_dict(orient='records')

def stream_to_kafka(lap_data):
    for lap in lap_data:
        producer.send(TOPIC, value=lap)
        print(f"Sent lap {lap.get('LapNumber')} of driver {lap.get('Driver')} to Kafka")
        time.sleep(0.5)  # simulate a stream, reduce if needed

if __name__ == '__main__':
    print("Loading lap data...")
    data = get_lap_data()
    print(f"Streaming {len(data)} laps to Kafka...")
    stream_to_kafka(data)
