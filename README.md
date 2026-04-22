# Smart City Traffic & Congestion Pipeline

> Scenario 1: Smart City Traffic Management System — Colombo, Sri Lanka

A production-style **Lambda Architecture** data pipeline that simulates real-time
traffic monitoring across 4 major junctions in Colombo. The system ingests live
sensor data, detects congestion in real-time, triggers critical alerts, and
generates nightly analytical reports for traffic police deployment decisions.

---

## Architecture

```
+------------------------------------------------------------------+
|                       LAMBDA ARCHITECTURE                        |
+------------------------------------------------------------------+
|                                                                  |
|   [Python Producer]                                              |
|   4 Junction Sensors                                             |
|         |                                                        |
|         v                                                        |
|   [Apache Kafka]  <--------- topic: traffic-raw                 |
|         |                                                        |
|         |                                                        |
|         +-------------------------+                              |
|         |                         |                             |
|         v                         v                             |
|   SPEED LAYER               BATCH LAYER                         |
|   [Spark Streaming]         [Apache Airflow]                    |
|   5-min windows             Nightly DAG @ 23:00                 |
|   Congestion Index          Peak Hour Analysis                  |
|   Critical Alerts           Police Report                       |
|         |                         |                             |
|         v                         v                             |
|   [PostgreSQL]              [reports/ folder]                   |
|   processed_traffic         traffic_report.csv                  |
|   critical_traffic          traffic_report.txt                  |
|                                                                  |
+------------------------------------------------------------------+
```

---

## Tech Stack

| Layer | Technology | Justification |
|---|---|---|
| **Ingestion** | Apache Kafka 7.6.1 | High-throughput message broker, supports replay, scales horizontally |
| **Stream Processing** | Apache Spark 3.5.3 Structured Streaming | SQL-like API, native watermarking for event time, windowing support |
| **Orchestration** | Apache Airflow 2.8.1 | DAG-based scheduling, retry logic, task dependency management |
| **Storage** | PostgreSQL 15 | Reliable relational store, easy to query from Airflow batch jobs |
| **Infrastructure** | Docker Compose | Reproducible local cluster, all services on shared network |

---

## Project Structure

```
smart-city-traffic/
|
+-- docker-compose.yml              # Full 8-container infrastructure
|
+-- postgres-init/
|   +-- init.sql                    # Creates 3 tables on first startup
|
+-- producer/
|   +-- producer.py                 # Simulates 4 Colombo junction sensors
|   +-- requirements.txt            # kafka-python
|
+-- spark/
|   +-- streaming_job.py            # Windowed aggregations + alert detection
|
+-- airflow/
|   +-- dags/
|   |   +-- traffic_nightly_dag.py  # 3-task nightly reporting DAG
|   +-- logs/                       # Airflow task logs (git-ignored)
|   +-- plugins/                    # Custom plugins (empty)
|
+-- reports/                        # Generated reports appear here
    +-- traffic_report_YYYY-MM-DD.csv
    +-- traffic_report_YYYY-MM-DD.txt
```

---

## Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- Python 3.9+

### 1. Clone the repository

```bash
git clone https://github.com/MadhaviHindagoda/smart-city-traffic.git
cd smart-city-traffic
```

### 2. Start all infrastructure containers

```bash
docker-compose up -d
```

Wait ~60 seconds then verify all 8 containers are running:

```bash
docker ps
```

Expected containers: `zookeeper`, `kafka`, `postgres`, `spark-master`,
`spark-worker`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`

### 3. Create Airflow admin user (first time only)

```bash
docker exec airflow-webserver airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```

### 4. Run the sensor data producer

```bash
cd producer
pip install -r requirements.txt
python producer.py
```

You will see live sensor readings every second:

```
2026-04-04 14:27:55 [INFO]   Sent  sensor=J1_Galle_Road    speed=50.6 km/h  count=166
2026-04-04 14:27:55 [WARNING] CRITICAL EVENT  sensor=J2_Kandy_Road  speed=8.8 km/h
```

### 5. Submit the Spark streaming job

Open a new terminal:

```bash
docker cp spark/streaming_job.py spark-master:/opt/spark/streaming_job.py

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.6.0 \
  /opt/spark/streaming_job.py
```

Expected output after ~30 seconds:

```
Spark session ready.
Both streaming queries running. Waiting for termination...
```

### 6. Trigger the nightly Airflow report

```bash
docker exec airflow-scheduler airflow dags trigger traffic_nightly_report
```

Or go to **http://localhost:8081** and find `traffic_nightly_report` then click the play button.

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8081 | admin / admin |
| Spark Master UI | http://localhost:8080 | none |
| PostgreSQL | localhost:5432 | airflow / airflow |

---

## Database Schema

### processed_traffic
Windowed aggregations written by Spark every 5 minutes.

| Column | Type | Description |
|---|---|---|
| window_start | TIMESTAMP | Start of the 5-minute window |
| window_end | TIMESTAMP | End of the 5-minute window |
| sensor_id | VARCHAR | Junction identifier |
| avg_speed | FLOAT | Average speed in km/h |
| vehicle_count | INTEGER | Total vehicles in window |
| congestion_index | FLOAT | Calculated CI (0 to 100) |

### critical_traffic
Written immediately when avg_speed drops below 10 km/h.

| Column | Type | Description |
|---|---|---|
| sensor_id | VARCHAR | Junction that triggered the alert |
| event_time | TIMESTAMP | Sensor event time |
| avg_speed | FLOAT | Speed that triggered the alert |
| alert_level | VARCHAR | Always CRITICAL |

### peak_hour_report
Written by Airflow nightly DAG.

| Column | Type | Description |
|---|---|---|
| report_date | DATE | Date of the report |
| sensor_id | VARCHAR | Junction |
| peak_hour | INTEGER | Hour with highest traffic (0-23) |
| max_vehicle_count | INTEGER | Vehicle count at peak hour |
| avg_congestion_index | FLOAT | Average CI for that hour |
| needs_police | BOOLEAN | True if CI is 70 or above |

---

## How It Works

### Stream Layer — Congestion Index

Every sensor reading is ingested via Kafka and processed by Spark in
5-minute tumbling windows based on **event time** (the sensor's own
timestamp, not Spark's processing clock).

```
Congestion Index = max(0, (1 - avg_speed / 80) x 100)
```

- CI = 0   means free flow at 80+ km/h
- CI = 50  means moderate congestion at around 40 km/h
- CI = 100 means complete standstill

If avg_speed drops below 10 km/h in any reading, an alert is written to
the `critical_traffic` table **immediately** without waiting for the window to close.

### Event Time vs Processing Time

Spark uses the sensor's `timestamp` field (event time) rather than the
system clock (processing time). A **10-minute watermark** is applied,
meaning Spark will wait up to 10 minutes for late-arriving data before
closing a window. This correctly handles network delays and sensor lag.

### Batch Layer — Airflow DAG

The `traffic_nightly_report` DAG runs at 23:00 daily with 3 sequential tasks:

```
aggregate_peak_hours --> flag_police_junctions --> generate_report
```

1. **aggregate_peak_hours** — Queries processed_traffic for the day and
   finds the hour with the highest average vehicle count per junction
2. **flag_police_junctions** — Marks junctions with CI of 70 or above as
   requiring police intervention and saves results to peak_hour_report table
3. **generate_report** — Writes traffic_report_YYYY-MM-DD.csv and a
   human-readable .txt summary to the reports/ folder

---

## Ethics and Data Governance

| Concern | Mitigation |
|---|---|
| **Privacy** | Sensor data contains no PII — no license plates, no driver identity |
| **Surveillance creep** | Pipeline is scoped strictly to junction-level aggregates, not individual tracking |
| **Data retention** | Raw Kafka messages auto-expire; PostgreSQL data should be purged after 90 days |
| **Access control** | critical_traffic table should be restricted to authorized traffic authority roles only |
| **Transparency** | Citizens should be informed that junction-level monitoring is in operation |
| **Misuse prevention** | Any extension to individual vehicle tracking requires legal oversight and consent |


