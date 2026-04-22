-- Create a separate database for traffic data
CREATE DATABASE traffic_db;

-- Connect to it
\c traffic_db;

-- Processed windowed aggregations from Spark
CREATE TABLE IF NOT EXISTS processed_traffic (
    id               SERIAL PRIMARY KEY,
    window_start     TIMESTAMP NOT NULL,
    window_end       TIMESTAMP NOT NULL,
    sensor_id        VARCHAR(60) NOT NULL,
    avg_speed        FLOAT,
    vehicle_count    INTEGER,
    congestion_index FLOAT,
    created_at       TIMESTAMP DEFAULT NOW()
);

-- Critical alerts: written immediately when avg_speed < 10 km/h
CREATE TABLE IF NOT EXISTS critical_traffic (
    id          SERIAL PRIMARY KEY,
    sensor_id   VARCHAR(60) NOT NULL,
    event_time  TIMESTAMP NOT NULL,
    avg_speed   FLOAT,
    vehicle_count INTEGER,
    alert_level VARCHAR(20) DEFAULT 'CRITICAL',
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Nightly report output (peak hours per junction)
CREATE TABLE IF NOT EXISTS peak_hour_report (
    id              SERIAL PRIMARY KEY,
    report_date     DATE NOT NULL,
    sensor_id       VARCHAR(60) NOT NULL,
    peak_hour       INTEGER,
    max_vehicle_count INTEGER,
    avg_congestion_index FLOAT,
    needs_police    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);