import json
import datetime
import os
import time
import requests

from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load environment variables from the .env file
load_dotenv()

# Kafka Producer Configuration (Inside Docker Network)
KAFKA_BROKERS = os.environ["KAFKA_BROKERS"]
producer_config = {
    "bootstrap.servers": KAFKA_BROKERS,
    "acks": "all",
    "retries": 5
}
producer = Producer(producer_config)

app = FastAPI(
    title="USGS Earthquake API → Kafka",
    description="Collect earthquakes from USGS and publish to Kafka",
    version="1.0.0"
)

USGS_ENDPOINT = "https://earthquake.usgs.gov/fdsnws/event/1/query"


@app.get("/health_check", summary="Health Check", tags=["Monitoring"])
async def health_check():
    return {"status": "success", "message": "Health check successful."}


def to_iso_date(d: datetime.date) -> str:
    # USGS expects YYYY-MM-DD
    return d.strftime("%Y-%m-%d")


@app.post("/send_data")
async def send_data(start_date: str, end_date: str, min_magnitude: float = 0.0):
    """
    - start_date (str): 'DD/MM/YYYY'
    - end_date   (str): 'DD/MM/YYYY'
    - min_magnitude (float): optional filter

    Fetch earthquakes from USGS between start_date and end_date (inclusive),
    then publish to Kafka topic: 'earthquakes'
    """
    # Parse input dates
    try:
        start_dt = datetime.datetime.strptime(start_date, "%d/%m/%Y").date()
        end_dt = datetime.datetime.strptime(end_date, "%d/%m/%Y").date()
    except ValueError:
        raise HTTPException(status_code=422, detail="Dates must be in format DD/MM/YYYY")

    if start_dt > end_dt:
        raise HTTPException(status_code=422, detail="start_date must be <= end_date")

    today = datetime.date.today()
    if end_dt > today:
        raise HTTPException(status_code=422, detail=f"End date must be <= {today.strftime('%d/%m/%Y')}")

    # Build USGS query
    params = {
        "format": "geojson",
        "starttime": to_iso_date(start_dt),
        "endtime": to_iso_date(end_dt),
        "minmagnitude": min_magnitude,
        "orderby": "time-asc"  # from oldest to newest
    }

    try:
        resp = requests.get(USGS_ENDPOINT, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"USGS request failed: {str(e)}")
    except ValueError:
        raise HTTPException(status_code=502, detail="USGS response was not valid JSON")

    features = payload.get("features", [])
    total_sent = 0

    try:
        for feat in features:
            props = feat.get("properties", {})
            geom = feat.get("geometry", {})
            quake_id = feat.get("id")

            # Build a clean document for Elasticsearch later
            quake_obj = {
                "event_id": quake_id,
                "title": props.get("title"),
                "place": props.get("place"),
                "magnitude": props.get("mag"),
                "time_ms": props.get("time"),  # epoch ms
                "time_iso": (
                    datetime.datetime.utcfromtimestamp(props["time"] / 1000).isoformat() + "Z"
                    if props.get("time") else None
                ),
                "updated_ms": props.get("updated"),
                "url": props.get("url"),
                "tsunami": props.get("tsunami"),
                "type": props.get("type"),
                "coordinates": geom.get("coordinates"),  # [lon, lat, depth]
                "source": "usgs"
            }

            # Send to Kafka
            producer.produce(
                "earthquakes",
                key=str(quake_id),
                value=json.dumps(quake_obj)
            )
            producer.poll(0)  # allow delivery callbacks / serve internal queue
            total_sent += 1

        producer.flush(10)  # ensure all messages are sent
        return {
            "status": "success",
            "result": {
                "total_events_fetched": len(features),
                "total_events_sent_to_kafka": total_sent,
                "topic": "earthquakes"
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka publish failed: {str(e)}")