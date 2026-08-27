"""
Script One-Off: Fetch BusServices Reference Data from LTA DataMall
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
logger = logging.getLogger("fetch-bus-services")

LTA_ACCOUNT_KEY = os.environ.get("LTA_ACCOUNT_KEY")
LTA_BASE_URL = os.environ.get("LTA_BASE_URL", "https://datamall2.mytransport.sg/ltaodataservice")
TARGET_SERVICES = [s.strip() for s in os.environ.get("BUS_SERVICE_NOS", "190,147,2").split(",")]
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
BACKOFF_BASE_SECONDS = int(os.environ.get("BACKOFF_BASE_SECONDS", "2"))

HEADERS = {
    "AccountKey": LTA_ACCOUNT_KEY,
    "accept": "application/json",
}

OUTPUT_FILE = os.path.join(SCRIPT_DIR, "bus_services_reference.json")


def fetch_service(service_no: str) -> list[dict]:
    url = f"{LTA_BASE_URL}/BusServices"
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params={"ServiceNo": service_no}, timeout=10)
            if resp.status_code == 429:
                wait = BACKOFF_BASE_SECONDS * (2 ** attempt)
                logger.warning(f"429 rate limited on ServiceNo={service_no}. Backing off {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json().get("value", [])
        except requests.RequestException as e:
            logger.error(f"Request failed for ServiceNo={service_no}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(BACKOFF_BASE_SECONDS * (2 ** attempt))
    return []


def fetch_all_bus_services() -> list[dict]:
    services = []
    for service_no in TARGET_SERVICES:
        records = fetch_service(service_no)
        for record in records:
            services.append({
                "service_no": record["ServiceNo"],
                "operator": record["Operator"],
                "direction": record["Direction"],
                "category": record["Category"],
                "am_peak_freq": record.get("AM_Peak_Freq"),
                "am_offpeak_freq": record.get("AM_Offpeak_Freq"),
                "pm_peak_freq": record.get("PM_Peak_Freq"),
                "pm_offpeak_freq": record.get("PM_Offpeak_Freq"),
                "loop_desc": record.get("LoopDesc"),
            })
        logger.info(f"ServiceNo={service_no}: {len(records)} record(s) fetched (direction variants included)")

    return services


def main():
    if not LTA_ACCOUNT_KEY:
        logger.error("LTA_ACCOUNT_KEY tidak ditemukan di environment variable.")
        return

    logger.info(f"Fetching BusServices reference data untuk rute: {TARGET_SERVICES}")
    services_data = fetch_all_bus_services()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(services_data, f, indent=2)

    logger.info(f"Berhasil menyimpan {len(services_data)} records reference data ke {OUTPUT_FILE}")


if __name__ == "__main__":
    main()