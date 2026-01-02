CREATE TABLE IF NOT EXISTS hourly_stats (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50),
    window_start TIMESTAMP,
    avg_volume DOUBLE PRECISION,
    avg_speed DOUBLE PRECISION,
    record_count INT
);

CREATE TABLE IF NOT EXISTS daily_stats (
    id SERIAL PRIMARY KEY,
    date DATE,
    peak_volume INT,
    peak_sensor_id VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS sensor_availability (
    id SERIAL PRIMARY KEY,
    date DATE,
    total_sensors_seen INT,
    availability_percent DOUBLE PRECISION
);