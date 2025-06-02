import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

IDENTIFIER = "serhii_mishovych"
TOPIC = f"building_sensors_{IDENTIFIER}"

# Unique sensor ID for each run
sensor_id = random.randint(1000, 9999)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"🌡 Sensor {sensor_id} started...")

try:
    while True:
        temperature = random.uniform(25, 45)
        humidity = random.uniform(15, 85)
        timestamp = datetime.utcnow().isoformat()

        data = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2)
        }

        print(f"> Sending: {data}")
        producer.send(TOPIC, value=data)
        time.sleep(2)

except KeyboardInterrupt:
    print("🛑 Sensor stopped.")
