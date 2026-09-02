"""
Script One-Off: Load MinIO Bronze (bus_arrival parquet) -> MotherDuck Staging.
"""

import argparse
import logging
import os

import duckdb
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, "..", ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("load-bronze-to-staging")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000").replace("http://", "").replace("https://", "")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "urban-transit")

MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN")
MOTHERDUCK_DATABASE = os.environ.get("MOTHERDUCK_DATABASE", "urban_transit")

BRONZE_GLOB = f"s3://{MINIO_BUCKET}/bronze/bus_arrival/**/*.parquet"

DEDUP_KEYS = ["bus_stop_code", "service_no", "next_bus_eta", "event_ts"]

def build_local_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        CREATE SECRET minio_secret (
            TYPE s3,
            KEY_ID '{MINIO_ROOT_USER}',
            SECRET '{MINIO_ROOT_PASSWORD}',
            ENDPOINT '{MINIO_ENDPOINT}',
            USE_SSL false,
            URL_STYLE 'path'
        );
    """)
    return con


def load(since: str | None) -> None:
    con = build_local_connection()
    where_clause = ""
    if since:
        where_clause = f"WHERE (year || '-' || month || '-' || day) >= '{since}'"
        logger.info(f"Filtering partisi bronze dengan --since {since}")
    logger.info(f"Reading bronze parquet from {BRONZE_GLOB} ...")
    raw_count = con.execute(f"SELECT count(*) FROM read_parquet('{BRONZE_GLOB}') {where_clause}").fetchone()[0]
    logger.info(f"Read {raw_count} raw rows.")

    if raw_count == 0:
        logger.warning("Bronze kosong — pastikan spark-consumer sudah jalan & ada event ter-ingest.")
        con.close()
        return
    dedup_key_cols = ", ".join(DEDUP_KEYS)
    con.execute(f"""
        CREATE TEMP TABLE bronze_dedup AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *,
                   row_number() OVER (PARTITION BY {dedup_key_cols} ORDER BY event_ts DESC) AS rn
            FROM read_parquet('{BRONZE_GLOB}')
            {where_clause}
        )
        WHERE rn = 1
    """)
    local_count = con.execute("SELECT count(*) FROM bronze_dedup").fetchone()[0]
    logger.info(f"Deduped locally: {local_count} rows ready to push to MotherDuck.")
  
    con.execute("INSTALL motherduck; LOAD motherduck;")
    con.execute(f"SET motherduck_token='{MOTHERDUCK_TOKEN}';")
    con.execute(f"ATTACH 'md:{MOTHERDUCK_DATABASE}' AS md;")

    con.execute("INSERT INTO md.staging.stg_bus_arrival_raw BY NAME SELECT * FROM bronze_dedup")

    inserted = con.execute("SELECT count(*) FROM md.staging.stg_bus_arrival_raw").fetchone()[0]
    logger.info(f"Done. staging.stg_bus_arrival_raw now has {inserted} total rows.")

    con.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", help="Filter partisi bronze, format YYYY-MM-DD (opsional)")
    args = parser.parse_args()

    if not MOTHERDUCK_TOKEN:
        logger.error("MOTHERDUCK_TOKEN tidak ditemukan di .env — jalankan motherduck/setup_motherduck.py dulu.")
        return

    load(args.since)


if __name__ == "__main__":
    main()