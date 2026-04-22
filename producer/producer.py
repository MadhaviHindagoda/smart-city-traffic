"""
Smart City Traffic Producer
Simulates 4 junction sensors in Colombo sending data every second.
Critical traffic events (avg_speed < 10 km/h) are injected ~5% of the time.
"""

import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Logging setup ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
TOPIC          = "traffic-raw"
INTERVAL_SEC   = 1          # emit every 1 second
CRITICAL_PROB  = 0.05       # 5% chance of critical traffic per sensor per cycle

JUNCTIONS = {
    "J1_Galle_Road":    {"base_speed": 45, "base_count": 120},
    "J2_Kandy_Road":    {"base_speed": 55, "base_count":  90},
    "J3_Baseline_Road": {"base_speed": 35, "base_count": 150},
    "J4_Rajagiriya":    {"base_speed": 50, "base_count": 110},
}

# ── Connect to Kafka with retry ────────────────────────────────────────
def create_producer(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            log.info("Connected to Kafka broker at %s", KAFKA_BROKER)
            return producer
        except NoBrokersAvailable:
            log.warning("Broker not ready. Attempt %d/%d — retrying in %ds", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple attempts.")

# ── Data generation ────────────────────────────────────────────────────
def generate_reading(sensor_id: str, config: dict) -> dict:
    """
    Generate one sensor reading.
    - Normal: speed varies ±30% around the base, count varies ±40%
    - Critical: speed drops to 2–9 km/h (traffic jam simulation)
    """
    is_critical = random.random() < CRITICAL_PROB

    if is_critical:
        avg_speed     = round(random.uniform(2.0, 9.9), 2)
        vehicle_count = random.randint(180, 250)   # jam → many vehicles stopped
        log.warning("⚠  CRITICAL EVENT  sensor=%s  speed=%.1f km/h", sensor_id, avg_speed)
    else:
        speed_var     = config["base_speed"] * 0.30
        count_var     = config["base_count"] * 0.40
        avg_speed     = round(random.uniform(
            config["base_speed"] - speed_var,
            config["base_speed"] + speed_var), 2)
        vehicle_count = random.randint(
            int(config["base_count"] - count_var),
            int(config["base_count"] + count_var))

    return {
        "sensor_id":     sensor_id,
        "timestamp":     datetime.utcnow().isoformat(),   # ISO-8601 event time
        "vehicle_count": vehicle_count,
        "avg_speed":     avg_speed,
        "is_critical":   is_critical,
    }

# ── Main loop ──────────────────────────────────────────────────────────
def main():
    log.info("Starting Smart City Traffic Producer")
    log.info("Topic: %s | Junctions: %d | Interval: %ds", TOPIC, len(JUNCTIONS), INTERVAL_SEC)

    producer = create_producer()
    cycle    = 0

    try:
        while True:
            cycle += 1
            log.info("── Cycle %d ──────────────────────────────", cycle)

            for sensor_id, config in JUNCTIONS.items():
                reading = generate_reading(sensor_id, config)
                producer.send(TOPIC, key=sensor_id, value=reading)
                log.info("  Sent  sensor=%-22s speed=%6.1f km/h  count=%d",
                         reading["sensor_id"], reading["avg_speed"], reading["vehicle_count"])

            producer.flush()
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        log.info("Producer stopped by user.")
    finally:
        producer.close()
        log.info("Kafka producer closed.")

if __name__ == "__main__":
    main()