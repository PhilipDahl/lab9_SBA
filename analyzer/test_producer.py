from pykafka import KafkaClient
import json

client = KafkaClient(hosts="localhost:9092")
topic = client.topics[b"events"]
producer = topic.get_sync_producer()

# Listing event
producer.produce(json.dumps({
    "event_type": "listing",
    "data": {
        "id": 1,
        "title": "Example Listing"
    }
}).encode("utf-8"))

# Transaction event
producer.produce(json.dumps({
    "event_type": "transaction",
    "data": {
        "id": 101,
        "amount": 5000
    }
}).encode("utf-8"))

print("✅ Events sent!")
