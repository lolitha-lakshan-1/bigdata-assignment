import json
import time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import psycopg2
from psycopg2.extras import execute_values

KAFKA_BROKER = 'localhost:9092'
RAW_TOPIC = 'traffic.raw'
PROCESSED_TOPIC = 'traffic.processed'

DB_CONFIG = {
    "host": "localhost",
    "database": "traffic_db",
    "user": "user",
    "password": "password"
}

def update_running_totals(hourly_stats, daily_state, data):
    sensor_id = data['sensor_id']
    volume = data['volume']
    speed = data.get('speed', 0)

    if sensor_id not in hourly_stats:
        hourly_stats[sensor_id] = {'sum_vol': 0, 'sum_speed': 0, 'count': 0}
    
    hourly_stats[sensor_id]['sum_vol'] += volume
    hourly_stats[sensor_id]['sum_speed'] += speed
    hourly_stats[sensor_id]['count'] += 1

    if volume > daily_state['peak_volume']:
        daily_state['peak_volume'] = volume
        daily_state['peak_sensor'] = sensor_id
    
    daily_state['active_sensors_today'].add(sensor_id)
    daily_state['all_known_sensors'].add(sensor_id)

def calculate_hourly_averages(hourly_stats, window_start):
    db_rows = []
    kafka_messages = []

    for sensor_id, stats in hourly_stats.items():
        if stats['count'] == 0: continue

        avg_vol = stats['sum_vol'] / stats['count']
        avg_speed = stats['sum_speed'] / stats['count']

        db_rows.append((sensor_id, window_start, avg_vol, avg_speed, stats['count']))

        kafka_messages.append({
            "type": "HOURLY_STAT",
            "window_start": str(window_start),
            "sensor_id": sensor_id,
            "avg_volume": round(avg_vol, 2),
            "avg_speed": round(avg_speed, 2)
        })
    
    return db_rows, kafka_messages

def calculate_daily_summary(daily_state, date):
    total_known = len(daily_state['all_known_sensors'])
    active_today = len(daily_state['active_sensors_today'])
    
    if total_known > 0:
        avail_pct = (active_today / total_known) * 100
    else:
        avail_pct = 0.0

    summary = {
        "date": str(date),
        "peak_volume": daily_state['peak_volume'],
        "peak_sensor": daily_state['peak_sensor'],
        "active_count": active_today,
        "availability_pct": avail_pct
    }
    return summary

def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            return conn
        except Exception as e:
            print("Waiting for Database...", e)
            time.sleep(5)

def process_stream():
    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    db_conn = get_db_connection()
    cursor = db_conn.cursor()

    hourly_stats = {} 
    daily_state = {
        'peak_volume': 0, 
        'peak_sensor': None, 
        'active_sensors_today': set(),
        'all_known_sensors': set()
    }
    
    current_hour = None
    current_day = None

    print("Processor started...")

    for message in consumer:
        data = message.value
        
        try:
            event_dt = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
        except ValueError:
            event_dt = datetime.fromisoformat(data['timestamp'])
            
        event_hour = event_dt.replace(minute=0, second=0, microsecond=0)
        event_day = event_dt.date()

        if current_hour is None:
            current_hour = event_hour
            current_day = event_day

        if event_hour > current_hour:
            print(f"--- Closing Hour: {current_hour} ---")
            db_rows, kafka_msgs = calculate_hourly_averages(hourly_stats, current_hour)
            
            if db_rows:
                execute_values(cursor, 
                    "INSERT INTO hourly_stats (sensor_id, window_start, avg_volume, avg_speed, record_count) VALUES %s", 
                    db_rows)
                db_conn.commit()
                
                for msg in kafka_msgs:
                    producer.send(PROCESSED_TOPIC, value=msg)
                
                print(f"Processed stats for {len(db_rows)} sensors.")

            hourly_stats = {}
            current_hour = event_hour

        if event_day > current_day:
            print(f"--- Closing Day: {current_day} ---")
            summary = calculate_daily_summary(daily_state, current_day)
            
            cursor.execute("INSERT INTO daily_stats (date, peak_volume, peak_sensor_id) VALUES (%s, %s, %s)", 
                           (summary['date'], summary['peak_volume'], str(summary['peak_sensor'])))
            cursor.execute("INSERT INTO sensor_availability (date, total_sensors_seen, availability_percent) VALUES (%s, %s, %s)", 
                           (summary['date'], summary['active_count'], summary['availability_pct']))
            db_conn.commit()

            summary['type'] = "DAILY_SUMMARY"
            producer.send(PROCESSED_TOPIC, value=summary)

            daily_state['peak_volume'] = 0
            daily_state['peak_sensor'] = None
            daily_state['active_sensors_today'] = set()
            current_day = event_day

        update_running_totals(hourly_stats, daily_state, data)

if __name__ == "__main__":
    process_stream()