import logging
import logging.config
import yaml
import connexion
import httpx
import json
import datetime
import uuid
from pykafka import KafkaClient
from connexion import NoContent
from flask import request  # Make sure this is at the top with other imports

# Load logging configuration from YAML file
with open("/app/config/receiver/receiver_log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
logging.config.dictConfig(LOG_CONFIG)

# Create logger from the basicLogger defined in the YAML configuration
logger = logging.getLogger('basicLogger')

# Load app_conf.yml for Kafka details
with open('/app/config/receiver/receiver_conf.yaml', 'r') as f:
    config = yaml.safe_load(f.read())

# Kafka client setup
kafka_hostname = config['events']['kafka']['hostname']
kafka_port = config['events']['kafka']['port']
kafka_topic = config['events']['kafka']['topic']

client = KafkaClient(hosts=f'{kafka_hostname}:{kafka_port}')
topic = client.topics[str.encode(kafka_topic)]
producer = topic.get_sync_producer()

def submit_listing_event(body):

    trace_id = body.get('trace_id') or str(uuid.uuid4())  # Generate or use provided trace_id

    # Convert the timestamp to datetime object
    timestamp = body.get('timestamp')
    if timestamp:
        try:
            timestamp = datetime.datetime.fromisoformat(timestamp)  # Parse timestamp string into datetime
        except ValueError:
            return {"message": "Invalid timestamp format"}, 400
    else:
        return {"message": "Missing 'timestamp' field"}, 400

    # Extracting other necessary fields from the body
    user_id = body.get('user_id')
    item_id = body.get('item_id')
    price = body.get('price')

    # Error handling for missing fields
    if not user_id or not item_id or not price:
        return {"message": "Missing required fields"}, 400

    # Log received event
    logger.info(f"Received event listing with trace id of {trace_id}")

    # Prepare the message for Kafka
    reading = {
        "trace_id": trace_id,
        "user_id": user_id,
        "item_id": item_id,
        "price": price,
        "timestamp": timestamp.isoformat(),  # Send as string again in ISO format
    }

    msg = {
        "type": "listing_event",  # Event type: listing_event
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": reading
    }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))  # Send message to Kafka

    # Return success response
    return NoContent, 201

def submit_transaction_event(body):

    trace_id = body.get('trace_id') or str(uuid.uuid4())  # Generate or use provided trace_id

    timestamp = body.get('timestamp')
    if timestamp:
        try:
            timestamp = datetime.datetime.fromisoformat(timestamp)  # Parse timestamp string into datetime
        except ValueError:
            return {"message": "Invalid timestamp format"}, 400
    else:
        return {"message": "Missing 'timestamp' field"}, 400

    user_id = body.get('user_id')
    transaction_id = body.get('transaction_id')
    amount = body.get('amount')

    # Error handling for missing fields
    if not user_id or not transaction_id or not amount:
        return {"message": "Missing required fields"}, 400

    # Log received event
    logger.info(f"Received event transaction with trace id of {trace_id}")

    # Prepare the message for Kafka
    reading = {
        "trace_id": trace_id,
        "user_id": user_id,
        "transaction_id": transaction_id,
        "amount": amount,
        "timestamp": timestamp.isoformat(),
    }

    msg = {
        "type": "transaction_event",  # Event type: transaction_event
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": reading
    }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))  # Send message to Kafka

    # Return success response
    return NoContent, 201

def get_listings(start_timestamp, end_timestamp):
    return [], 200

def get_transactions(start_timestamp, end_timestamp):
    return [], 200

# Flask app with connexion
app = connexion.FlaskApp(__name__, specification_dir='')
# Register the API using the OpenAPI specification (openapi.yaml)
app.add_api('openapi.yaml', strict_validation=False, validate_responses=True)

flask_app = app.app

@flask_app.route("/debug", methods=["POST"])
def debug():
    raw = request.data.decode()
    print("DEBUG BODY:\n", raw)
    return {"message": "Received"}, 200

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")
