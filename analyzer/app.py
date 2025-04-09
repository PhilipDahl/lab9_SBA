import json
import logging
import yaml
from pykafka import KafkaClient
import connexion
from flask import request  # Still used internally by Connexion

# Load configuration from a YAML file
def load_config():
    with open("/app/config/analyzer/analyzer_config.yaml", "r") as file:
        return yaml.safe_load(file)

# Set up logging
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger(__name__)

# Initialize config and logger
CONFIG = load_config()
logger = setup_logging()

# Function to get event from Kafka by index
def get_event_by_index(event_type, index):
    try:
        client = KafkaClient(hosts=CONFIG["kafka"]["hostname"])
        topic = client.topics[CONFIG["kafka"]["topic"].encode()]
        consumer = topic.get_simple_consumer(reset_offset_on_start=True, consumer_timeout_ms=1000)

        logger.info(f"Fetching {event_type} event with index {index} from Kafka")

        counter = 0
        for msg in consumer:
            if msg is None:
                continue
            data = json.loads(msg.value.decode("utf-8"))

            if data.get("type") == event_type:  # <-- FIXED: matches what Receiver sends
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
    return get_event_by_index("listing_event", int(index))  # <-- FIXED type

def get_transaction_event():
    index = request.args.get("index")
    if index is None or not index.isdigit():
        return {"message": "Invalid or missing 'index' parameter"}, 400
    return get_event_by_index("transaction_event", int(index))  # <-- FIXED type

def get_event_stats():
    try:
        client = KafkaClient(hosts=CONFIG["kafka"]["hostname"])
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

# Connexion app setup
app = connexion.App(__name__, specification_dir='.')
app.add_api("openapi.yaml")

if __name__ == "__main__":
    logger.info("Starting Connexion analyzer app")
    app.run(port=8081, host="0.0.0.0")
