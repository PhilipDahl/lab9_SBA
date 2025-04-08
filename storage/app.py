import yaml
import logging
import logging.config
from datetime import datetime
from db_class import SubmitListingEvent, SubmitTransactionEvent
from db_setup import get_session
import threading
from kafka import KafkaConsumer
import uuid
import json
import os
import connexion
from flask import request, jsonify

# === Load Service Config ===
with open("/config/storage_config.yml", "r") as f:
    config = yaml.safe_load(f)

db_config = config['datastore']
kafka_config = config['kafka']

# === Logging Setup ===
SERVICE_NAME = "storage"

with open("/config/log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f)

os.makedirs("/logs", exist_ok=True)

LOG_CONFIG['handlers']['file_handler']['filename'] = f'/logs/{SERVICE_NAME}.log'
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(SERVICE_NAME)
logger.info(f"{SERVICE_NAME.capitalize()} service started")

# === Kafka Configuration ===
KAFKA_SERVER = f"{kafka_config['hostname']}:{kafka_config['port']}"
KAFKA_TOPIC = kafka_config['topic']

# === Kafka Consumer Thread ===
def process_messages():
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER)
    for message in consumer:
        logger.info(f"Processing message: {message.value.decode('utf-8')}")
        event_data = message.value.decode('utf-8')

        if "listing" in event_data:
            try:
                body = parse_listing_event(event_data)
                process_listing_event(body)
            except Exception as e:
                logger.error(f"Error processing listing event: {e}")

        elif "transaction" in event_data:
            try:
                body = parse_transaction_event(event_data)
                process_transaction_event(body)
            except Exception as e:
                logger.error(f"Error processing transaction event: {e}")

def setup_kafka_thread():
    t1 = threading.Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()

# === Dummy Parsers (for Lab testing) ===
def parse_listing_event(event_data):
    return {
        'user_id': 'user_123',
        'item_id': 'item_123',
        'price': 100.00,
        'timestamp': datetime.now().isoformat(),
        'trace_id': str(uuid.uuid4())
    }

def parse_transaction_event(event_data):
    return {
        'user_id': 'user_123',
        'transaction_id': 'txn_123',
        'amount': 200.00,
        'timestamp': datetime.now().isoformat(),
        'trace_id': str(uuid.uuid4())
    }

# === Event Processing Functions ===
def process_listing_event(body):
    session = get_session()
    try:
        listing_event = SubmitListingEvent(
            user_id=body['user_id'],
            item_id=body['item_id'],
            price=body['price'],
            timestamp=body['timestamp'],
            trace_id=body['trace_id']
        )
        session.add(listing_event)
        session.commit()
        logger.debug(f"Stored listing: item {body['item_id']} (trace_id={body['trace_id']})")
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to store listing event: {e}")
    finally:
        session.close()

def process_transaction_event(body):
    session = get_session()
    try:
        transaction_event = SubmitTransactionEvent(
            user_id=body['user_id'],
            transaction_id=body['transaction_id'],
            amount=body['amount'],
            timestamp=body['timestamp'],
            trace_id=body['trace_id']
        )
        session.add(transaction_event)
        session.commit()
        logger.debug(f"Stored transaction: {body['transaction_id']} (trace_id={body['trace_id']})")
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to store transaction event: {e}")
    finally:
        session.close()

# === REST API Endpoints ===
def get_listings():
    logger.info("GET /events/listings")
    session = get_session()
    start_timestamp = request.args.get('start_timestamp')
    end_timestamp = request.args.get('end_timestamp')

    try:
        start = datetime.fromisoformat(start_timestamp)
        end = datetime.fromisoformat(end_timestamp)
    except (ValueError, TypeError):
        return jsonify({"message": "Invalid timestamp format"}), 400

    listings = session.query(SubmitListingEvent).filter(
        SubmitListingEvent.timestamp >= start,
        SubmitListingEvent.timestamp < end
    ).all()

    session.close()
    return jsonify([listing.to_dict() for listing in listings]), 200

def get_transactions():
    logger.info("GET /events/transactions")
    session = get_session()
    start_timestamp = request.args.get('start_timestamp')
    end_timestamp = request.args.get('end_timestamp')

    try:
        start = datetime.fromisoformat(start_timestamp)
        end = datetime.fromisoformat(end_timestamp)
    except (ValueError, TypeError):
        return jsonify({"message": "Invalid timestamp format"}), 400

    transactions = session.query(SubmitTransactionEvent).filter(
        SubmitTransactionEvent.timestamp >= start,
        SubmitTransactionEvent.timestamp < end
    ).all()

    session.close()
    return jsonify([transaction.to_dict() for transaction in transactions]), 200

# === Health/Home Endpoint ===
def home():
    return "✅ Storage service is running!"

# === Connexion App Setup ===
app = connexion.FlaskApp(__name__, specification_dir='.')
app.add_api('openapi.yaml')
app.app.add_url_rule('/', 'home', home)

if __name__ == '__main__':
    setup_kafka_thread()
    app.run(port=8090, host="0.0.0.0")
