"""
One-off: provision MotherDuck database + Staging/Gold schemas + staging tables.
"""
import logging
import os

import duckdb
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, "..", ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("setup-motherduck")

MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
MOTHERDUCK_DATABASE = os.environ.get("MOTHERDUCK_DATABASE", "urban_transit")

CREATE_STG_BUS_ARRIVAL = """
CREATE TABLE IF NOT EXISTS staging.stg_bus_arrival_raw (
    ingested_at             VARCHAR,
    bus_stop_code           VARCHAR,
    service_no              VARCHAR,
    operator                VARCHAR,
    origin_code             VARCHAR,
    destination_code        VARCHAR,
    next_bus_eta             VARCHAR,
    next_bus_load            VARCHAR,
    next_bus2_eta            VARCHAR,
    direction                INTEGER,
    am_peak_freq_low         DOUBLE,
    am_peak_freq_high        DOUBLE,
    am_offpeak_freq_low      DOUBLE,
    am_offpeak_freq_high     DOUBLE,
    pm_peak_freq_low         DOUBLE,
    pm_peak_freq_high        DOUBLE,
    pm_offpeak_freq_low      DOUBLE,
    pm_offpeak_freq_high     DOUBLE,
    period_bucket             VARCHAR,
    expected_high_minutes     DOUBLE,
    headway_actual_seconds    BIGINT,
    headway_gap_seconds       BIGINT,
    event_ts                  TIMESTAMP,
    year                      VARCHAR,
    month                     VARCHAR,
    day                       VARCHAR,
    hour                      VARCHAR
);
"""

CREATE_STG_BUS_SERVICES = """
CREATE TABLE IF NOT EXISTS staging.stg_bus_services_raw (
    service_no        VARCHAR,
    operator           VARCHAR,
    direction           INTEGER,
    category            VARCHAR,
    am_peak_freq        VARCHAR,
    am_offpeak_freq     VARCHAR,
    pm_peak_freq        VARCHAR,
    pm_offpeak_freq     VARCHAR,
    loop_desc           VARCHAR
);
"""

CREATE_STG_BUS_ROUTES = """
CREATE TABLE IF NOT EXISTS staging.stg_bus_routes_raw (
    bus_stop_code    VARCHAR,
    service_no       VARCHAR,
    direction        INTEGER,
    stop_sequence    INTEGER
);
"""

def main():
    if not MOTHERDUCK_TOKEN:
        logger.error("MOTHERDUCK_TOKEN tidak ditemukan di .env — generate dulu di motherduck.com.")
        return

    logger.info(f"Connecting to MotherDuck (database={MOTHERDUCK_DATABASE}) ...")
    con = duckdb.connect(f"md:?motherduck_token={MOTHERDUCK_TOKEN}")

    con.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DATABASE}")
    con.execute(f"USE {MOTHERDUCK_DATABASE}")
    logger.info(f"Database {MOTHERDUCK_DATABASE} ready.")

    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    logger.info("Schemas ready: staging, gold (gold left empty — dbt materializes into it).")

    con.execute(CREATE_STG_BUS_ARRIVAL)
    con.execute(CREATE_STG_BUS_SERVICES)
    con.execute(CREATE_STG_BUS_ROUTES)
    logger.info("Staging tables ensured: stg_bus_arrival_raw, stg_bus_services_raw, stg_bus_routes_raw.")

    tables = con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema IN ('staging', 'gold') ORDER BY 1, 2"
    ).fetchall()
    for schema, table in tables:
        logger.info(f"  - {schema}.{table}")

    con.close()
    logger.info("MotherDuck setup selesai.")


if __name__ == "__main__":
    main()