import json
import yaml
import logging
import logging.config
from pykafka import KafkaClient
import connexion
from flask import request  # Used by Connexion
import os

# === Setup Service Name for Logging ===
SERVICE_NAME = "analyzer"

# === Load LOGGING config ===
with open("/config/log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f)

os.makedirs("/logs", exist_ok=True)

LOG_CONFIG['handlers']['file_handler']['filename'] = f'/logs/{SERVICE_NAME}.log'
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(SERVICE_NAME)
logger.info(f"{SERVICE_NAME.capitalize()} service started")

# === Load SERVICE config ===
with open("/config/analyzer_config.yml", "r") as file:
    CONFIG = yaml.safe_load(file)

# === Kafka Utilities ===
def get_event_by_index(event_type, index):
    try:
        client = KafkaClient(hosts=f"{CONFIG['kafka']['hostname']}:{CONFIG['kafka']['port']}")
        topic = client.topics[CONFIG["kafka"]["topic"].encode()]
        consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

        logger.info(f"Fetching {event_type} event with index {index}")

        counter = 0
        for msg in consumer:
            if msg is None:
                continue
            data = json.loads(msg.value.decode("utf-8"))
            if data.get("type") == event_type:
                if counter == index:
                    return data, 200
                counter += 1

        logger.warning(f"No {event_type} event found at index {index}")
        return {"message": f"No {event_type} event at index {index}!"}, 404

    except Exception as e:
        logger.error(f"Error while retrieving event: {str(e)}")
        return {"message": "Internal Server Error"}, 500

def get_listing_event():
    index = request.args.get("index")
    if index is None or not index.isdigit():
        return {"message": "Invalid or missing 'index' parameter"}, 400
    return get_event_by_index("listing_event", int(index))

def get_transaction_event():
    index = request.args.get("index")
    if index is None or not index.isdigit():
        return {"message": "Invalid or missing 'index' parameter"}, 400
    return get_event_by_index("transaction_event", int(index))

def get_event_stats():
    try:
        client = KafkaClient(hosts=f"{CONFIG['kafka']['hostname']}:{CONFIG['kafka']['port']}")
        topic = client.topics[CONFIG["kafka"]["topic"].encode()]
        consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

        listing_count = 0
        transaction_count = 0

        for msg in consumer:
            data = json.loads(msg.value.decode("utf-8"))
            if data.get("type") == "listing_event":
                listing_count += 1
            elif data.get("type") == "transaction_event":
                transaction_count += 1

        return {
            "num_listing_events": listing_count,
            "num_transaction_events": transaction_count
        }, 200

    except Exception as e:
        logger.error(f"Error while fetching event stats: {str(e)}")
        return {"message": "Internal Server Error"}, 500

# === Health/Home Endpoint ===
def home():
    return "✅ Storage service is running!"

# === Connexion App Setup ===
app = connexion.FlaskApp(__name__, specification_dir='.')
app.add_api("openapi.yaml")

if __name__ == "__main__":
    logger.info("Starting Connexion analyzer app")
    app.run(port=8110, host="0.0.0.0")
