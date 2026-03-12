# 01_generate_data.py
# Generates synthetic playback event data

import pandas as pd
import random
from datetime import datetime, timedelta

NUM_EVENTS = 10000

event_types = ["play", "pause", "seek", "complete"]
devices = ["smart_tv", "mobile", "tablet", "laptop"]
countries = ["US", "UK", "CA", "AU"]

rows = []

for i in range(NUM_EVENTS):
    rows.append({
        "event_id": f"evt_{i}",
        "user_id": f"user_{random.randint(1,1000)}",
        "session_id": f"sess_{random.randint(1,500)}",
        "content_id": f"movie_{random.randint(1,200)}",
        "event_type": random.choice(event_types),
        "device_type": random.choice(devices),
        "country": random.choice(countries),
        "watch_seconds": random.randint(10,600),
        "event_timestamp": datetime.now() - timedelta(minutes=random.randint(0,10000))
    })

df = pd.DataFrame(rows)

df.to_csv("data/generated/playback_events.csv", index=False)

print("Synthetic playback data generated.")