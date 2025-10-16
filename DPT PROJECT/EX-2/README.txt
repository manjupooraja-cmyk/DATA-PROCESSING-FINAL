EXERCISE – 2
REAL-TIME DATA STREAMING USING APACHE KAFKA
-------------------------------------------------------

OBJECTIVE:
To build a real-time producer-consumer system using Apache Kafka.

FOLDER:
C:\Users\Manju\DPT PROJECT\EX-2

-------------------------------------------------------
STEPS TO RUN:

1. Start Zookeeper:
   zookeeper-server-start.sh config/zookeeper.properties

2. Start Kafka Broker:
   kafka-server-start.sh config/server.properties

3. Create Kafka Topic:
   kafka-topics.sh --create --topic sensor-stream --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3

4. Verify Topic:
   kafka-topics.sh --list --bootstrap-server localhost:9092

-------------------------------------------------------
RUN PRODUCER:

cd "C:\Users\Manju\DPT PROJECT\EX-2"
python producer.py

(This sends random sensor data every 2 seconds.)

-------------------------------------------------------
RUN CONSUMER:

Open another terminal:
cd "C:\Users\Manju\DPT PROJECT\EX-2"
python consumer.py

(Displays rolling average of last 10 readings per sensor.)

-------------------------------------------------------
SAMPLE OUTPUT:

Producer:
Sent: {'sensor_id': 'S3', 'temperature': 31.4, 'humidity': 62.1, 'timestamp': '2025-10-16 15:20:10'}

Consumer:
Sensor: S3 | Temp: 31.40°C | AvgTemp(10): 30.75°C | AvgHum: 61.90% | Status: ✅ Normal
Sensor: S1 | Temp: 38.10°C | AvgTemp(10): 36.20°C | AvgHum: 57.20% | Status: 🔥 Overheated

-------------------------------------------------------
RESULT:
✅ Producer sends live JSON data to Kafka topic.
✅ Consumer receives, aggregates, and analyzes stream in real-time.

END OF EXERCISE 2
-------------------------------------------------------
