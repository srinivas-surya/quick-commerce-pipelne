import random, time, json, uuid, datetime as dt

def jitter(val, scale=0.0005):
    return val + random.uniform(-scale, scale)

def simulate_path(courier_id="C123", start=(12.9716, 77.5946), steps=50, interval=0.5):
    lat, lon = start
    for _ in range(steps):
        lat = jitter(lat)
        lon = jitter(lon)
        yield {
            "courier_id": courier_id,
            "lat": lat,
            "lon": lon,
            "ts": dt.datetime.utcnow().isoformat() + "Z"
        }
        time.sleep(interval)

if __name__ == "__main__":
    for e in simulate_path():
        print(json.dumps(e))
