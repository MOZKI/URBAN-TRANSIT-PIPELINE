"""
Script One-Off: Load reference JSON (bus_services_reference.json,
bus_routes_reference.json) -> MotherDuck Staging.
"""
import logging
import os

import duckdb
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, "..", ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("load-reference-to-staging")

MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
MOTHERDUCK_DATABASE = os.environ.get("MOTHERDUCK_DATABASE", "urban_transit")

REFERENCE_FILES = {
    "stg_bus_services_raw": os.path.join(SCRIPT_DIR, "bus_services_reference.json"),
    "stg_bus_routes_raw": os.path.join(SCRIPT_DIR, "bus_routes_reference.json"),
    "stg_bus_stops_raw": os.path.join(SCRIPT_DIR, "bus_stops_reference.json"),
}


def main():
    if not MOTHERDUCK_TOKEN:
        logger.error("MOTHERDUCK_TOKEN tidak ditemukan di .env — jalankan motherduck/setup_motherduck.py dulu.")
        return

    con = duckdb.connect(f"md:{MOTHERDUCK_DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}")

    for table_name, file_path in REFERENCE_FILES.items():
        if not os.path.exists(file_path):
            logger.warning(f"{file_path} tidak ditemukan — skip {table_name}.")
            continue

        logger.info(f"Loading {file_path} -> staging.{table_name} ...")
        con.execute(f"""
            CREATE OR REPLACE TABLE staging.{table_name} AS
            SELECT * FROM read_json_auto('{file_path}')
        """)
        count = con.execute(f"SELECT count(*) FROM staging.{table_name}").fetchone()[0]
        logger.info(f"Done. staging.{table_name} now has {count} rows.")

    con.close()


if __name__ == "__main__":
    main()