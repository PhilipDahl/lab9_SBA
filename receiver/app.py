import logging
import logging.config
import yaml
import connexion
import httpx
import json
import datetime
from connexion import NoContent
from pykafka import KafkaClient
from pykafka.exceptions import NoBrokersAvailableError
import uuid
import os
import time

# === Setup service name ===
SERVICE_NAME = "receiver"

# === Logging setup ===
with open("/config/log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f)

os.makedirs("/logs", exist_ok=True)
LOG_CONFIG['handlers']['file_handler']['filename'] = f'/logs/{SERVICE_NAME}.log'
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(SERVICE_NAME)
logger.info(f"{SERVICE_NAME.capitalize()} service started")

# === Load app config ===
with open("/config/receiver_config.yml", "r") as f:
    config = yaml.safe_load(f)

kafka_config = config['kafka']
kafka_hostname = config['kafka']['hostname']
kafka_port = config['kafka']['port']
kafka_topic = config['kafka']['topic']

# === Kafka client with retry ===
MAX_RETRIES = 10
for attempt in range(MAX_RETRIES):
    try:
        client = KafkaClient(hosts=f'{kafka_hostname}:{kafka_port}')
        topic = client.topics[str.encode(kafka_topic)]
        producer = topic.get_sync_producer()
        logger.info("Connected to Kafka successfully")
        break
    except NoBrokersAvailableError as e:
        logger.warning(f"Kafka not available (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
        time.sleep(2 ** attempt)
else:
    logger.error("Could not connect to Kafka after multiple retries. Exiting.")
    raise SystemExit(1)

# === API endpoint handlers ===
def submit_listing_event(body):
    trace_id = body.get('trace_id') or str(uuid.uuid4())
    timestamp = body.get('timestamp')
    if timestamp:
        try:
            timestamp = datetime.datetime.fromisoformat(timestamp)
        except ValueError:
            return {"message": "Invalid timestamp format"}, 400
    else:
        return {"message": "Missing 'timestamp' field"}, 400

    user_id = body.get('user_id')
    item_id = body.get('item_id')
    price = body.get('price')

    if not user_id or not item_id or not price:
        return {"message": "Missing required fields"}, 400

    logger.info(f"Received event listing with trace id of {trace_id}")

    reading = {
        "trace_id": trace_id,
        "user_id": user_id,
        "item_id": item_id,
        "price": price,
        "timestamp": timestamp.isoformat(),
    }

    msg = {
        "type": "listing_event",
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": reading
    }

    producer.produce(json.dumps(msg).encode("utf-8"))
    return NoContent, 201

def submit_transaction_event(body):
    trace_id = body.get('trace_id') or str(uuid.uuid4())
    timestamp = body.get('timestamp')
    if timestamp:
        try:
            timestamp = datetime.datetime.fromisoformat(timestamp)
        except ValueError:
            return {"message": "Invalid timestamp format"}, 400
    else:
        return {"message": "Missing 'timestamp' field"}, 400

    user_id = body.get('user_id')
    transaction_id = body.get('transaction_id')
    amount = body.get('amount')

    if not user_id or not transaction_id or not amount:
        return {"message": "Missing required fields"}, 400

    logger.info(f"Received event transaction with trace id of {trace_id}")

    reading = {
        "trace_id": trace_id,
        "user_id": user_id,
        "transaction_id": transaction_id,
        "amount": amount,
        "timestamp": timestamp.isoformat(),
    }

    msg = {
        "type": "transaction_event",
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": reading
    }

    producer.produce(json.dumps(msg).encode("utf-8"))
    return NoContent, 201

def get_listings(start_timestamp, end_timestamp):
    return [], 200

def get_transactions(start_timestamp, end_timestamp):
    return [], 200

# === Health/Home Endpoint ===
def home():
    return "✅ Reciever service is running!"
# === Connexion app using uvicorn backend ===

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")
