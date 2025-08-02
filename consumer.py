import sys
from kafka import KafkaConsumer

if len(sys.argv) < 2:
    print("Usage: python consumer.py <group_id>")
    sys.exit(1)

group_id = sys.argv[1]
topic_name = 'my-test-topic'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:19092', 'localhost:19093', 'localhost:19094'],
    group_id=group_id,
    auto_offset_reset='earliest',
    client_id=f'consumer-{group_id}'
)


print(f"Consumer started in group '{group_id}'. Waiting for messages...")

for message in consumer:
    print(
        f"Group: {group_id} | "
        f"Partition: {message.partition} | "
        f"Offset: {message.offset} | "
        f"Key: {message.key.decode('utf-8')} | "
        f"Value: {message.value.decode('utf-8')}"
    )