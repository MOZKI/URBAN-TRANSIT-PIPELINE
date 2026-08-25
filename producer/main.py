"""
LTA Bus Arrival Producer — Polling Mode = `corridor`
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))  # load .env 

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("lta-producer")

# config
LTA_ACCOUNT_KEY = os.environ["LTA_ACCOUNT_KEY"]
LTA_BASE_URL = os.environ.get("LTA_BASE_URL", "https://datamall2.mytransport.sg/ltaodataservice")

POLL_MODE = os.environ.get("POLL_MODE", "manual")
BUS_SERVICE_NOS = os.environ.get("BUS_SERVICE_NOS", "").split(",")
BUS_STOP_CODES_MANUAL = os.environ.get("BUS_STOP_CODES", "83139").split(",")

POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
REQUEST_STAGGER_MS = int(os.environ.get("REQUEST_STAGGER_MS", "200"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
BACKOFF_BASE_SECONDS = int(os.environ.get("BACKOFF_BASE_SECONDS", "2"))

REDPANDA_BROKER = os.environ.get("REDPANDA_BROKER", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "lta.bus-arrival.raw")

HEADERS = {
    "AccountKey": LTA_ACCOUNT_KEY,
    "accept": "application/json",
}


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=REDPANDA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=3,
        acks="all",
    )


def request_with_backoff(url: str, params: dict) -> dict | None:
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
            if resp.status_code == 429:
                wait = BACKOFF_BASE_SECONDS * (2 ** attempt)
                logger.warning(f"429 rate limited. Backing off {wait}s (attempt {attempt + 1})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.error(f"Request failed ({url}, params={params}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_BASE_SECONDS * (2 ** attempt))
    return None


def resolve_corridor_stops(service_nos: list[str]) -> list[str]:
    target_services = {s.strip() for s in service_nos}
    stop_codes: set[str] = set()

    skip = 0
    while True:
        data = request_with_backoff(f"{LTA_BASE_URL}/BusRoutes", {"$skip": skip})
        if not data or not data.get("value"):
            break

        records = data["value"]
        for record in records:
            if record["ServiceNo"] in target_services:
                stop_codes.add(record["BusStopCode"])

        logger.info(f"Fetched page at skip={skip} ({len(records)} records). Matched so far: {len(stop_codes)}")

        if len(records) < 500:
            break  
        skip += 500

    return sorted(stop_codes)


def fetch_bus_arrival(bus_stop_code: str) -> dict | None:
    url = f"{LTA_BASE_URL}/v3/BusArrival"
    return request_with_backoff(url, {"BusStopCode": bus_stop_code})


def enrich_event(raw: dict, bus_stop_code: str) -> dict:
    return {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "bus_stop_code": bus_stop_code,
        "payload": raw,
    }


def run():
    producer = build_producer()

    if POLL_MODE == "corridor":
        stop_codes = resolve_corridor_stops(BUS_SERVICE_NOS)
        logger.info(f"Corridor mode: polling {len(stop_codes)} deduped stops for services {BUS_SERVICE_NOS}")
    else:
        stop_codes = [s.strip() for s in BUS_STOP_CODES_MANUAL]
        logger.info(f"Manual mode: polling {len(stop_codes)} stops")

    stagger_seconds = REQUEST_STAGGER_MS / 1000.0

    while True:
        cycle_start = time.time()

        for stop_code in stop_codes:
            raw = fetch_bus_arrival(stop_code)
            if raw is None:
                continue

            event = enrich_event(raw, stop_code)
            try:
                producer.send(TOPIC_NAME, key=stop_code, value=event)
            except KafkaError as e:
                logger.error(f"Gagal publish ke Redpanda ({stop_code}): {e}")

            time.sleep(stagger_seconds)  

        producer.flush()

        elapsed = time.time() - cycle_start
        sleep_remaining = max(0, POLL_INTERVAL_SECONDS - elapsed)
        logger.info(f"Cycle done in {elapsed:.1f}s. Sleeping {sleep_remaining:.1f}s until next cycle.")
        time.sleep(sleep_remaining)


if __name__ == "__main__":
    run()