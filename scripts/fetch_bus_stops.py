"""
Script One-Off: Fetch BusStops Reference Data 
from LTA DataMall and save as a local JSON lookup table.
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
logger = logging.getLogger("fetch-bus-stops")

LTA_ACCOUNT_KEY = os.environ.get("LTA_ACCOUNT_KEY")
LTA_BASE_URL = os.environ.get("LTA_BASE_URL", "https://datamall2.mytransport.sg/ltaodataservice")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
BACKOFF_BASE_SECONDS = int(os.environ.get("BACKOFF_BASE_SECONDS", "2"))

HEADERS = {
    "AccountKey": LTA_ACCOUNT_KEY,
    "accept": "application/json",
}

ROUTES_FILE = os.path.join(SCRIPT_DIR, "bus_routes_reference.json")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "bus_stops_reference.json")


def load_target_stop_codes() -> set[str]:
    if not os.path.exists(ROUTES_FILE):
        logger.error(f"{ROUTES_FILE} tidak ditemukan — jalankan fetch_bus_routes.py dulu.")
        return set()

    with open(ROUTES_FILE, encoding="utf-8") as f:
        routes = json.load(f)

    return {r["bus_stop_code"] for r in routes}


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


def fetch_all_bus_stops(target_codes: set[str]) -> list[dict]:
    stops = []
    found_codes = set()
    skip = 0

    while True:
        data = request_with_backoff(f"{LTA_BASE_URL}/BusStops", {"$skip": skip})
        if not data or not data.get("value"):
            break

        records = data["value"]
        for record in records:
            code = record["BusStopCode"]
            if code in target_codes and code not in found_codes:
                stops.append({
                    "bus_stop_code": code,
                    "road_name": record.get("RoadName"),
                    "description": record.get("Description"),
                    "latitude": record.get("Latitude"),
                    "longitude": record.get("Longitude"),
                })
                found_codes.add(code)

        logger.info(
            f"Fetched page at skip={skip} ({len(records)} records). "
            f"Matched so far: {len(stops)}/{len(target_codes)}"
        )

        if len(records) < 500:
            break
        if found_codes == target_codes:
            logger.info("Semua target stop code sudah ketemu — stop pagination lebih awal.")
            break
        skip += 500

    missing = target_codes - found_codes
    if missing:
        logger.warning(f"{len(missing)} bus_stop_code tidak ketemu di /BusStops: {sorted(missing)}")

    return stops


def main():
    if not LTA_ACCOUNT_KEY:
        logger.error("LTA_ACCOUNT_KEY tidak ditemukan di environment variable.")
        return

    target_codes = load_target_stop_codes()
    if not target_codes:
        return

    logger.info(f"Fetching BusStops reference data untuk {len(target_codes)} halte target.")
    stops_data = fetch_all_bus_stops(target_codes)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(stops_data, f, indent=2)

    logger.info(f"Berhasil menyimpan {len(stops_data)} records reference data ke {OUTPUT_FILE}")


if __name__ == "__main__":
    main()