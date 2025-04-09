import yaml
import logging
import logging.config
from flask import Flask, jsonify
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler
import os
import json
import requests

# Set up basic logging configuration
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('basicLogger')

app = Flask(__name__)

# Load configuration from the YAML file
def load_config():
    try:
        with open('/app/config/processing/processing_conf.yaml', 'r') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        logger.error("Config file not found!")
        raise Exception("Configuration file not found, terminating!")
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise Exception("Error parsing configuration file, terminating!")

config = load_config()

# Retrieve the configuration values
stats_file_path = config.get('datastore', {}).get('filename', 'data.json')
scheduler_interval = config.get('scheduler', {}).get('interval', 60)
listings_url = config.get('eventstores', {}).get('listings', {}).get('url', 'http://localhost:8090/events/listings')
transactions_url = config.get('eventstores', {}).get('transactions', {}).get('url', 'http://localhost:8090/events/transactions')

def populate_stats():
    logger.info("Starting periodic processing...")

    # Default statistics in case the file doesn't exist
    default_stats = {
        "num_listings": 0,
        "num_transactions": 0,
        "last_processed_timestamp": "1970-01-01T00:00:00"  # Start from epoch if no file
    }

    # Read current statistics from the JSON file (or use defaults)
    try:
        if os.path.exists(stats_file_path) and os.path.getsize(stats_file_path) > 0:
            with open(stats_file_path, 'r') as f:
                stats = json.load(f)
        else:
            logger.info("Stats file is empty or not found. Using default values.")
            stats = default_stats
    except Exception as e:
        logger.error(f"Error occurred while reading stats file: {e}")
        stats = default_stats

    # Get the current datetime as the "end" timestamp
    current_timestamp = datetime.now(timezone.utc).isoformat()

    # Get the datetime of the most recent event processed from the stats file
    last_processed_timestamp = stats.get('last_processed_timestamp', "1970-01-01T00:00:00")
    
    logger.debug(f"Fetching new events since: {last_processed_timestamp}")

    # Initialize counters for events
    num_listings = 0
    num_transactions = 0

    try:
        # Get listings events
        listings_response = requests.get(listings_url, params={'start_timestamp': last_processed_timestamp, 'end_timestamp': current_timestamp})
        if listings_response.status_code == 200:
            listings_data = listings_response.json()
            num_listings = len(listings_data)
            logger.info(f"Received {num_listings} new listings.")
        else:
            logger.error(f"Failed to fetch listings. Status code: {listings_response.status_code}")

        # Get transaction events
        transactions_response = requests.get(transactions_url, params={'start_timestamp': last_processed_timestamp, 'end_timestamp': current_timestamp})
        if transactions_response.status_code == 200:
            transactions_data = transactions_response.json()
            num_transactions = len(transactions_data)
            logger.info(f"Received {num_transactions} new transactions.")
        else:
            logger.error(f"Failed to fetch transactions. Status code: {transactions_response.status_code}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error occurred while fetching events: {e}")
        return

    # Update statistics based on the new events
    updated_stats = {
        "num_listings": stats["num_listings"] + num_listings,
        "num_transactions": stats["num_transactions"] + num_transactions,
        "last_processed_timestamp": current_timestamp  # Update the last processed timestamp
    }

    # Write the updated statistics to the JSON file
    try:
        with open(stats_file_path, 'w') as f:
            json.dump(updated_stats, f, indent=4)
        logger.debug(f"Updated statistics: {updated_stats}")
    except Exception as e:
        logger.error(f"Failed to write updated statistics to file: {e}")

    logger.info("Periodic processing completed.")

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=scheduler_interval, coalesce=True, max_instances=1)
    sched.start()


@app.route('/')
def status():
    """API endpoint to check the status of the application."""
    return jsonify({"status": "running", "last_processed_timestamp": date_created})


if __name__ == "__main__":
    # Start the scheduler before running the API service
    init_scheduler()
    app.run(debug=True, port=8100, host="0.0.0.0")
