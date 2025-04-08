from pykafka import KafkaClient
import json
from datetime import datetime

client = KafkaClient(hosts="kafka:9092")
topic = client.topics[b"events"]
producer = topic.get_sync_producer()

# Listing event
producer.produce(json.dumps({
    "type": "listing_event",
    "datetime": datetime.now().isoformat(),
    "payload": {
        "trace_id": "abc123",
        "timestamp": datetime.now().isoformat(),
        "user_id": "u001",
        "item_id": "item001",
        "price": 100.00
    }
}).encode("utf-8"))

# Transaction event
producer.produce(json.dumps({
    "type": "transaction_event",
    "datetime": datetime.now().isoformat(),
    "payload": {
        "trace_id": "def456",
        "timestamp": datetime.now().isoformat(),
        "user_id": "u002",
        "transaction_id": "txn001",
        "amount": 200.00
    }
}).encode("utf-8"))

print("✅ Events sent!")
