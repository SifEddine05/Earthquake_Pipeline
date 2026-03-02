import time
import requests
import logging
from dotenv import load_dotenv
import os
import datetime
import schedule
import json

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables
load_dotenv()

# ---- SERVICES ENDPOINTS (Docker network) ----
EARTHQUAKE_API = os.getenv("EARTHQUAKE_API", "http://earthquake-api:8000")
HEALTH_ENDPOINT = f"{EARTHQUAKE_API}/health_check"
SEND_DATA_ENDPOINT = f"{EARTHQUAKE_API}/send_data"

# Elasticsearch
ES_ENDPOINT = os.getenv("ES_ENDPOINT", "http://elasticsearch:9200")

# Schedule hour (format HH:MM, e.g. "02:00")
REQUEST_NEW_DATA_HOURS = os.environ["REQUEST_NEW_DATA_HOURS"]

# Optional: minimum magnitude filter
MIN_MAGNITUDE = float(os.getenv("MIN_MAGNITUDE", "0.0"))

SEP = "-" * 30

# Load mappings (adapt the file path/name to your project)
# Example expected structure:
# [
#   {"index": "earthquakes", "mapping": {...settings+mappings...}}
# ]
with open("schedulerService/mappings.json", encoding="utf-8") as f:
    MAPPINGS = json.load(f)


def check_health(endpoint: str) -> bool:
    """Ping a service until it becomes available (max ~30s)."""
    max_retry = 30
    for i in range(max_retry):
        try:
            r = requests.get(endpoint, timeout=5)
            if r.status_code == 200:
                logging.info(f"Successfully pinged {endpoint}")
                return True
            logging.info(f"Ping failed ({r.status_code}) {endpoint} retry={i+1}/{max_retry}")
        except Exception as e:
            logging.info(f"Ping error {endpoint} retry={i+1}/{max_retry}, error={e}")
        time.sleep(1)

    raise RuntimeError(f"Impossible to ping {endpoint} after {max_retry} retries")


def ensure_index_exists(mapping_configuration: dict):
    """Check if an Elasticsearch index exists and create it if not."""
    index_name = mapping_configuration["index"]
    mapping = mapping_configuration["mapping"]

    r = requests.get(f"{ES_ENDPOINT}/{index_name}", timeout=10)
    if r.status_code == 200:
        logging.info(f"Index '{index_name}' already exists.")
        return

    logging.info(f"Index '{index_name}' does not exist. Creating...")
    create_r = requests.put(f"{ES_ENDPOINT}/{index_name}", json=mapping, timeout=20)

    if create_r.status_code in (200, 201):
        logging.info(f"Index '{index_name}' created successfully!")
    else:
        logging.error(f"Failed to create index '{index_name}'. Error: {create_r.status_code} {create_r.text}")


def send_data_to_kafka():
    """
    Calls the FastAPI endpoint that fetches USGS data and publishes it to Kafka.
    This is the 'regular extraction' step.
    """
    try:
        start_time = time.time()
        logging.info(f"Triggering Earthquake API to send data to Kafka... {SEP}")

        # Example: fetch "today" only (you can change to last 1 day, last 7 days, etc.)
        date_today = datetime.datetime.today().strftime("%d/%m/%Y")

        url = (
            f"{SEND_DATA_ENDPOINT}"
            f"?start_date={date_today}&end_date={date_today}"
            f"&min_magnitude={MIN_MAGNITUDE}"
        )

        logging.info(f"Calling: {url}")
        r = requests.post(url, timeout=300)  # can take time if many events

        duration_minutes = (time.time() - start_time) / 60

        if r.status_code == 200:
            logging.info(f"Success response: {r.text}")
        else:
            logging.error(f"Failed to send data: {r.status_code} {r.text}")

        logging.info(f"Duration: {duration_minutes:.2f} minutes")
        logging.info(SEP)

    except Exception as e:
        logging.error(f"Error triggering API: {e}")
        logging.info(SEP)


# ---- Scheduling ----
my_schedule = schedule.Scheduler()
my_schedule.every().day.at(REQUEST_NEW_DATA_HOURS).do(send_data_to_kafka)

if __name__ == "__main__":
    # 1) Check FastAPI is running
    check_health(HEALTH_ENDPOINT)

    # 2) Check Elasticsearch is running
    check_health(ES_ENDPOINT)

    # 3) Ensure indices/mappings exist before data flows
    for mapping_configuration in MAPPINGS:
        ensure_index_exists(mapping_configuration=mapping_configuration)

    # 4) Run scheduled jobs forever
    logging.info(f"Scheduler started. Daily trigger at {REQUEST_NEW_DATA_HOURS}. MIN_MAGNITUDE={MIN_MAGNITUDE}")
    while True:
        my_schedule.run_pending()
        time.sleep(5)