# cdc_consumer_incremental.py
from kafka import KafkaConsumer
import json
from river import linear_model, preprocessing, metrics
import joblib
import os
import time

# Settings
topic = "cdc-sensor"
bootstrap = "localhost:9092"
model_path = "online_model.joblib"

# Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[bootstrap],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=1000
)

# Create an online pipeline: StandardScaler -> LogisticRegression
model = preprocessing.StandardScaler() | linear_model.LogisticRegression()

# Load saved model if exists
if os.path.exists(model_path):
    model = joblib.load(model_path)
    print("Loaded saved model from", model_path)

# Metric for online evaluation
metric = metrics.Accuracy()

# In-memory store of latest records (simulate state store)
state = {}

# Counter for checkpointing
processed = 0

print("CDC Consumer started - listening to:", topic)
try:
    for msg in consumer:
        payload = msg.value
        op = payload.get("op")

        if op == "c" or op == "u":
            after = payload.get("after")
            if not after:
                continue
            rec_id = after["id"]
            features = {
                "temperature": float(after["temperature"]),
                "humidity": float(after["humidity"])
            }
            # Define label for demo: overheated if temperature > 35
            label = 1 if features["temperature"] > 35 else 0

            # Update state (upsert)
            state[rec_id] = features

            # Learn incrementally
            model = model.learn_one(features, label)
            pred = model.predict_one(features)
            metric.update(label, pred)

            print(f"[{op}] id={rec_id} temp={features['temperature']} hum={features['humidity']} "
                  f"label={label} pred={pred} acc={metric.get():.3f}")

        elif op == "d":
            before = payload.get("before")
            if not before:
                continue
            rec_id = before["id"]
            if rec_id in state:
                del state[rec_id]
                print(f"[d] id={rec_id} deleted from state")

        # Increment processed counter and periodically persist model
        processed += 1
        if processed % 50 == 0:
            joblib.dump(model, model_path)
            print("Model checkpoint saved to", model_path)

except KeyboardInterrupt:
    print("Shutting down CDC consumer")

finally:
    # Final save
    joblib.dump(model, model_path)
    print("Final model saved to", model_path)
    consumer.close()
