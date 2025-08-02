import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:19092', 'localhost:19093', 'localhost:19094'],
    client_id='my-producer'
)

topic_name = 'my-test-topic'
print(f"Sending messages to topic: {topic_name}")

for i in range(20):
    key = f'key-{(i % 4)}'.encode('utf-8')
    message = f'Message number {i}'.encode('utf-8')

    print(f"Sending: Key={key.decode()}, Value={message.decode()}")

    producer.send(topic_name, key=key, value=message)

    time.sleep(0.5)

producer.flush()
print("All messages sent.")