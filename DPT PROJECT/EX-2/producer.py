from kafka import KafkaProducer
import json, time, random

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "sensor-stream"

# Generate synthetic sensor data
def generate_sensor_data():
    return {
        "sensor_id": f"S{random.randint(1,5)}",
        "temperature": round(random.uniform(20, 40), 2),
        "humidity": round(random.uniform(30, 80), 2),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

print("🚀 Starting Kafka Producer...")
try:
    while True:
        data = generate_sensor_data()
        producer.send(topic, data)
        print("Sent:", data)
        time.sleep(2)  # every 2 seconds
except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    producer.flush()
    producer.close()
