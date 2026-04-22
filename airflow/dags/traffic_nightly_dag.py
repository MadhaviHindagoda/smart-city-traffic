"""
Smart City Traffic — Nightly Airflow DAG

Schedule: every day at 11 PM (23:00)
Tasks:
  1. aggregate_peak_hours  — queries processed_traffic, finds peak hour per junction
  2. flag_police_junctions — marks junctions where CI > 70 as needing police
  3. generate_report       — writes a CSV + plain-text report to /opt/airflow/reports/
"""

import os
import csv
import logging
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ── DB connection ──────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "postgres",
    "port":     5432,
    "dbname":   "traffic_db",
    "user":     "airflow",
    "password": "airflow",
}

REPORTS_DIR       = "/opt/airflow/reports"
POLICE_CI_THRESHOLD = 70.0   # congestion index above this → police intervention

# ── Task 1: Aggregate peak hours ───────────────────────────────────────
def aggregate_peak_hours(**context):
    """
    For each junction, find the hour with the highest average vehicle count
    from yesterday's processed_traffic data.
    Pushes results via XCom for the next task.
    """
    run_date = context["ds"]   # YYYY-MM-DD string provided by Airflow

    sql = """
        SELECT
            sensor_id,
            EXTRACT(HOUR FROM window_start)::INTEGER   AS hour,
            AVG(vehicle_count)::INTEGER                AS avg_count,
            AVG(congestion_index)                      AS avg_ci
        FROM processed_traffic
        WHERE DATE(window_start) = %s
        GROUP BY sensor_id, EXTRACT(HOUR FROM window_start)
        ORDER BY sensor_id, avg_count DESC;
    """

    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    cur.execute(sql, (run_date,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Keep only the top row (peak hour) per junction
    peaks = {}
    for sensor_id, hour, avg_count, avg_ci in rows:
        if sensor_id not in peaks:
            peaks[sensor_id] = {
                "sensor_id":  sensor_id,
                "peak_hour":  hour,
                "max_count":  avg_count,
                "avg_ci":     round(float(avg_ci), 2),
            }

    log.info("Peak hours found: %s", peaks)
    context["ti"].xcom_push(key="peaks", value=peaks)
    return peaks

# ── Task 2: Flag junctions needing police ─────────────────────────────
def flag_police_junctions(**context):
    """
    Reads peaks from XCom, adds a 'needs_police' flag,
    and saves results to peak_hour_report table.
    """
    run_date = context["ds"]
    peaks    = context["ti"].xcom_pull(task_ids="aggregate_peak_hours", key="peaks")

    if not peaks:
        log.warning("No peak data found for %s — skipping.", run_date)
        return {}

    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    flagged = {}
    for sensor_id, data in peaks.items():
        needs_police = data["avg_ci"] >= POLICE_CI_THRESHOLD
        data["needs_police"] = needs_police

        cur.execute("""
            INSERT INTO peak_hour_report
                (report_date, sensor_id, peak_hour, max_vehicle_count,
                 avg_congestion_index, needs_police)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (run_date, sensor_id, data["peak_hour"],
              data["max_count"], data["avg_ci"], needs_police))

        flagged[sensor_id] = data
        log.info("Junction %s → CI=%.1f | police=%s",
                 sensor_id, data["avg_ci"], needs_police)

    conn.commit()
    cur.close()
    conn.close()

    context["ti"].xcom_push(key="flagged", value=flagged)
    return flagged

# ── Task 3: Generate report ────────────────────────────────────────────
def generate_report(**context):
    """
    Writes two output files to /opt/airflow/reports/:
      - traffic_report_YYYY-MM-DD.csv   (machine-readable)
      - traffic_report_YYYY-MM-DD.txt   (human-readable summary)
    """
    run_date = context["ds"]
    flagged  = context["ti"].xcom_pull(task_ids="flag_police_junctions", key="flagged")

    if not flagged:
        log.warning("No data to report for %s.", run_date)
        return

    os.makedirs(REPORTS_DIR, exist_ok=True)
    csv_path = os.path.join(REPORTS_DIR, f"traffic_report_{run_date}.csv")
    txt_path = os.path.join(REPORTS_DIR, f"traffic_report_{run_date}.txt")

    # CSV report
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "sensor_id", "peak_hour", "max_count", "avg_ci", "needs_police"
        ])
        writer.writeheader()
        for data in flagged.values():
            writer.writerow(data)
    log.info("CSV report written: %s", csv_path)

    # Text summary
    police_list = [s for s, d in flagged.items() if d["needs_police"]]

    with open(txt_path, "w") as f:
        f.write(f"SMART CITY TRAFFIC REPORT — {run_date}\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"{'Junction':<25} {'Peak Hour':>10} {'Max Vehicles':>14} {'Avg CI':>8} {'Police?':>8}\n")
        f.write("-" * 70 + "\n")
        for data in flagged.values():
            f.write(
                f"{data['sensor_id']:<25} "
                f"{data['peak_hour']:>10}:00 "
                f"{data['max_count']:>14} "
                f"{data['avg_ci']:>8.1f} "
                f"{'YES ⚠' if data['needs_police'] else 'No':>8}\n"
            )
        f.write("\n")
        if police_list:
            f.write("JUNCTIONS REQUIRING POLICE INTERVENTION TOMORROW:\n")
            for j in police_list:
                f.write(f"  → {j}\n")
        else:
            f.write("No junctions require police intervention tomorrow.\n")

    log.info("Text report written: %s", txt_path)

# ── DAG definition ─────────────────────────────────────────────────────
default_args = {
    "owner":            "traffic-team",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="traffic_nightly_report",
    description="Nightly peak-hour aggregation and police intervention report",
    schedule_interval="0 23 * * *",    # every day at 11 PM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["smart-city", "traffic"],
) as dag:

    t1 = PythonOperator(
        task_id="aggregate_peak_hours",
        python_callable=aggregate_peak_hours,
    )

    t2 = PythonOperator(
        task_id="flag_police_junctions",
        python_callable=flag_police_junctions,
    )

    t3 = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    t1 >> t2 >> t3   # sequential: each task waits for the previous