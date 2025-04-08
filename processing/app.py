import yaml
import logging
import logging.config
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler
import os
import json
import requests
import connexion


# === Service Name for Logging ===
SERVICE_NAME = "processing"

# === Load Logging Config ===
with open("/config/log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f)

os.makedirs("/logs", exist_ok=True)

LOG_CONFIG['handlers']['file_handler']['filename'] = f'/logs/{SERVICE_NAME}.log'
logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger(SERVICE_NAME)
logger.info(f"{SERVICE_NAME.capitalize()} service started")

# === Load Processing Config ===
def load_config():
    try:
        with open('/config/processing_config.yml', 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        raise

config = load_config()
stats_file_path = config['output']['stats_file']
scheduler_interval = config['processing']['interval']
listings_url = config['storage']['url'] + "/events/listings"
transactions_url = config['storage']['url'] + "/events/transactions"

# === Scheduled Function ===
def populate_stats():
    logger.info("Running scheduled stats update")

    default_stats = {
        "num_listings": 0,
        "num_transactions": 0,
        "last_processed_timestamp": "1970-01-01T00:00:00"
    }

    try:
        if os.path.exists(stats_file_path) and os.path.getsize(stats_file_path) > 0:
            with open(stats_file_path, 'r') as f:
                stats = json.load(f)
        else:
            logger.info("No stats file found, using default values")
            stats = default_stats
    except Exception as e:
        logger.error(f"Error reading stats file: {e}")
        stats = default_stats

    current_timestamp = datetime.now(timezone.utc).isoformat()
    last_processed_timestamp = stats.get('last_processed_timestamp', default_stats["last_processed_timestamp"])

    logger.debug(f"Getting events between {last_processed_timestamp} and {current_timestamp}")

    num_listings, num_transactions = 0, 0

    try:
        listings_resp = requests.get(listings_url, params={
            "start_timestamp": last_processed_timestamp,
            "end_timestamp": current_timestamp
        })

        if listings_resp.status_code == 200:
            listings_data = listings_resp.json()
            num_listings = len(listings_data)
            logger.info(f"Fetched {num_listings} new listings")
        else:
            logger.error(f"Failed to fetch listings: {listings_resp.status_code}")

        transactions_resp = requests.get(transactions_url, params={
            "start_timestamp": last_processed_timestamp,
            "end_timestamp": current_timestamp
        })

        if transactions_resp.status_code == 200:
            transactions_data = transactions_resp.json()
            num_transactions = len(transactions_data)
            logger.info(f"Fetched {num_transactions} new transactions")
        else:
            logger.error(f"Failed to fetch transactions: {transactions_resp.status_code}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error making requests: {e}")
        return

    updated_stats = {
        "num_listings": stats["num_listings"] + num_listings,
        "num_transactions": stats["num_transactions"] + num_transactions,
        "last_processed_timestamp": current_timestamp
    }

    try:
        with open(stats_file_path, 'w') as f:
            json.dump(updated_stats, f, indent=4)
        logger.debug(f"Updated stats written to file: {updated_stats}")
    except Exception as e:
        logger.error(f"Failed to write stats: {e}")

    logger.info("Scheduled processing complete")

# === Scheduler ===
def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=scheduler_interval, coalesce=True, max_instances=1)
    sched.start()


# === Health/Home Endpoint ===
def home():
    return "✅ Storage service is running!"

# === Connexion App ===
app = connexion.FlaskApp(__name__, specification_dir='.')
app.add_api("openapi.yaml")

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")
