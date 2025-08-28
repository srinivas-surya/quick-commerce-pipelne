import json, time, random, threading
from kafka import KafkaProducer
import yaml

CONFIG = yaml.safe_load(open("config/app_config.yaml"))
BOOTSTRAP = CONFIG["kafka"]["bootstrap_servers"]
GPS_TOPIC = CONFIG["kafka"]["gps_topic"]
STATUS_TOPIC = CONFIG["kafka"]["status_topic"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def gps_stream(courier_id="C123"):
    lat, lon = 12.9715987, 77.594566
    while True:
        lat += random.uniform(-0.0005, 0.0005)
        lon += random.uniform(-0.0005, 0.0005)
        event = {"courier_id": courier_id, "lat": lat, "lon": lon, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        producer.send(GPS_TOPIC, event)
        time.sleep(1.0)

def status_stream(order_ids=(1001,1002,1003), courier_id="C123"):
    statuses = ["dispatched", "picked_up", "en_route", "delivered"]
    while True:
        event = {
            "order_id": random.choice(order_ids),
            "courier_id": courier_id,
            "status": random.choice(statuses),
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        producer.send(STATUS_TOPIC, event)
        time.sleep(2.0)

if __name__ == "__main__":
    t1 = threading.Thread(target=gps_stream, daemon=True)
    t2 = threading.Thread(target=status_stream, daemon=True)
    t1.start(); t2.start()
    print(f"Producing to topics: {GPS_TOPIC}, {STATUS_TOPIC} on {BOOTSTRAP}")
    while True:
        time.sleep(10)
