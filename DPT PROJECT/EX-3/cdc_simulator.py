# cdc_simulator.py
from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

topic = "cdc-sensor"

# generate create/update/delete CDC-like messages
def gen_create(id):
    return {"op": "c", "after": {"id": id, "temperature": round(random.uniform(20,45),2),
                                 "humidity": round(random.uniform(30,80),2),
                                 "ts": time.strftime("%Y-%m-%d %H:%M:%S")}}

def gen_update(id):
    return {"op": "u", "after": {"id": id, "temperature": round(random.uniform(20,45),2),
                                 "humidity": round(random.uniform(30,80),2),
                                 "ts": time.strftime("%Y-%m-%d %H:%M:%S")}}

def gen_delete(id):
    return {"op": "d", "before": {"id": id}}

print("CDC Simulator started. Sending events to topic:", topic)
try:
    id_counter = 1
    # create an initial set
    for i in range(1,6):
        msg = gen_create(i)
        producer.send(topic, msg)
    producer.flush()

    while True:
        action = random.choices(["create","update","delete"], weights=[0.1,0.8,0.1])[0]
        if action == "create":
            id_counter += 1
            msg = gen_create(id_counter)
        elif action == "update":
            id = random.randint(1, max(1, id_counter))
            msg = gen_update(id)
        else:
            id = random.randint(1, max(1, id_counter))
            msg = gen_delete(id)

        producer.send(topic, msg)
        print("Sent:", msg)
        time.sleep(1.0)   # send 1 event/sec
except KeyboardInterrupt:
    print("Stopped simulator")
finally:
    producer.flush()
    producer.close()
