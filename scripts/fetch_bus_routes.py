"""
Script One-Off: Fetch BusRoutes Reference Data from LTA DataMall
and save as a local JSON lookup table.
"""
import json
import logging
import os
import time

import requests
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, "..", ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("fetch-bus-routes")

LTA_ACCOUNT_KEY = os.environ.get("LTA_ACCOUNT_KEY")
LTA_BASE_URL = os.environ.get("LTA_BASE_URL", "https://datamall2.mytransport.sg/ltaodataservice")
TARGET_SERVICES = {s.strip() for s in os.environ.get("BUS_SERVICE_NOS", "190,147,2").split(",")}
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
BACKOFF_BASE_SECONDS = int(os.environ.get("BACKOFF_BASE_SECONDS", "2"))

HEADERS = {
    "AccountKey": LTA_ACCOUNT_KEY,
    "accept": "application/json",
}

OUTPUT_FILE = os.path.join(SCRIPT_DIR, "bus_routes_reference.json")


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
            logger.error(f"Request failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_BASE_SECONDS * (2 ** attempt))
    return None


def fetch_all_bus_routes() -> list[dict]:
    routes = []
    skip = 0
    while True:
        data = request_with_backoff(f"{LTA_BASE_URL}/BusRoutes", {"$skip": skip})
        if not data or not data.get("value"):
            break

        records = data["value"]
        for record in records:
            if record["ServiceNo"] in TARGET_SERVICES:
                routes.append({
                    "bus_stop_code": record["BusStopCode"],
                    "service_no": record["ServiceNo"],
                    "direction": record["Direction"],
                    "stop_sequence": record["StopSequence"],
                })

        logger.info(f"Fetched page at skip={skip} ({len(records)} records). Matched so far: {len(routes)}")

        if len(records) < 500:
            break
        skip += 500

    return routes


def main():
    if not LTA_ACCOUNT_KEY:
        logger.error("LTA_ACCOUNT_KEY tidak ditemukan di environment variable.")
        return

    logger.info(f"Fetching BusRoutes reference data untuk rute: {TARGET_SERVICES}")
    routes_data = fetch_all_bus_routes()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(routes_data, f, indent=2)

    logger.info(f"Berhasil menyimpan {len(routes_data)} records reference data ke {OUTPUT_FILE}")


if __name__ == "__main__":
    main()