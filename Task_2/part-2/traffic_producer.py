import time
import json
import pandas as pd
from kafka import KafkaProducer

KAFKA_TOPIC = "traffic.raw"
KAFKA_SERVER = 'localhost:9092'
CSV_FILE = 'data/Camera_Traffic_Counts.csv'

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=json_serializer
    )

    print(f"Loading data from {CSV_FILE}...")

    df = pd.read_csv(CSV_FILE, low_memory=False)
    df.columns = df.columns.str.strip()

    print("Cleaning data formats and dropping invalid rows...")

    if 'Volume' in df.columns:
        df['Volume'] = df['Volume'].astype(str).str.replace(',', '')
        df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
    else:
        print("Error: 'Volume' column not found.")
        return

    if 'ATD Device ID' in df.columns:
        df['ATD Device ID'] = df['ATD Device ID'].astype(str).str.replace(',', '')
        df['ATD Device ID'] = pd.to_numeric(df['ATD Device ID'], errors='coerce')
    else:
        print("Error: 'ATD Device ID' column not found.")
        return

    if 'Speed Average (Miles Per Hour)' in df.columns:
        df['Speed'] = pd.to_numeric(df['Speed Average (Miles Per Hour)'], errors='coerce')
    else:
        df['Speed'] = 0.0

    initial_count = len(df)
    df = df.dropna(subset=['Volume', 'ATD Device ID'])
    dropped_count = initial_count - len(df)
    print(f"Dropped {dropped_count} rows due to missing/invalid data.")

    df['Volume'] = df['Volume'].astype(int)
    df['ATD Device ID'] = df['ATD Device ID'].astype(int)

    print("Sorting by date...")
    df['Read Date'] = pd.to_datetime(df['Read Date'])
    df = df.sort_values(by='Read Date')

    print(f"Starting simulation. Replaying {len(df)} records...")

    count = 0
    try:
        for index, row in df.iterrows():
            # Build the kafka message
            message = {
                "sensor_id": str(row['ATD Device ID']),
                "timestamp": str(row['Read Date']),
                "volume": int(row['Volume']),
                "speed": float(row['Speed']) if pd.notna(row['Speed']) else 0.0,
                "intersection": str(row['Intersection Name']) if pd.notna(row['Intersection Name']) else "Unknown"
            }

            producer.send(
                KAFKA_TOPIC,
                key=message["sensor_id"].encode('utf-8'),
                value=message
            )

            count += 1
            print(f"Sent record {count}: Sensor {message['sensor_id']} | Vol: {message['volume']} | Speed: {message['speed']}")

            time.sleep(5)

        producer.flush()
        print("Simulation complete.")

    except KeyboardInterrupt:
        print("\nStopping simulation.")
    except Exception as e:
        print(f"Error sending message: {e}")

if __name__ == "__main__":
    run_producer()