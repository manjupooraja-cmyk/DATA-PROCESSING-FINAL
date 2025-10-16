from kafka import KafkaConsumer
import json
import pandas as pd
from collections import defaultdict

consumer = KafkaConsumer(
    'sensor-stream',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("📥 Starting Kafka Consumer...")
sensor_stats = defaultdict(list)

for msg in consumer:
    data = msg.value
    sensor_id = data['sensor_id']
    temperature = data['temperature']
    humidity = data['humidity']
    sensor_stats[sensor_id].append((temperature, humidity))

    # keep only recent 10 readings per sensor
    if len(sensor_stats[sensor_id]) > 10:
        sensor_stats[sensor_id].pop(0)

    # compute rolling average
    df = pd.DataFrame(sensor_stats[sensor_id], columns=['temp', 'hum'])
    avg_temp = df['temp'].mean()
    avg_hum = df['hum'].mean()

    print(f"Sensor: {sensor_id:>3} | "
          f"Latest Temp: {temperature:>5}°C | "
          f"Avg Temp (10 pts): {avg_temp:>5.2f}°C | "
          f"Avg Hum: {avg_hum:>5.2f}%")
