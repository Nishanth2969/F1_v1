from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'telemetry-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='f1-test-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for messages in 'telemetry-data'...")

for message in consumer:
    print("Received:", message.value)
