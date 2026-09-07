"""
Load MinIO Bronze (bus_arrival parquet) -> MotherDuck Staging.
"""

import argparse
import logging
import os

import duckdb
from dotenv import load_dotenv
import time

def retry_query(con, sql, params=None, attempts=5, base_delay=10):
    for i in range(attempts):
        try:
            return con.execute(sql, params) if params else con.execute(sql)
        except duckdb.IOException as e:
            if i == attempts - 1:
                raise
            wait = base_delay * (2 ** i)
            logger.warning(f"MinIO IO error (attempt {i+1}/{attempts}): {e} — retry in {wait}s")
            time.sleep(wait)

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
    con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    con.execute(f"SET s3_access_key_id='{MINIO_ROOT_USER}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_ROOT_PASSWORD}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    return con


def get_last_loaded_ts(con: duckdb.DuckDBPyConnection):
    try:
        row = con.execute("SELECT max(event_ts) FROM md.staging.stg_bus_arrival_raw").fetchone()
        return row[0] if row and row[0] is not None else None
    except duckdb.CatalogException:
        return None


def load(since: str | None) -> None:
    con = build_local_connection()
    con.execute("INSTALL motherduck; LOAD motherduck;")
    con.execute(f"SET motherduck_token='{MOTHERDUCK_TOKEN}';")
    con.execute(f"ATTACH 'md:{MOTHERDUCK_DATABASE}' AS md;")

    last_loaded_ts = get_last_loaded_ts(con)

    filters = []
    params: list = []

    if since:
        filters.append("(year || '-' || month || '-' || day) >= ?")
        params.append(since)
        logger.info(f"Filtering partisi bronze dengan --since {since}")
    elif last_loaded_ts is not None:
        filters.append("(year || '-' || month || '-' || day) >= ?")
        params.append(last_loaded_ts.strftime("%Y-%m-%d"))
        filters.append("event_ts > ?")
        params.append(last_loaded_ts)
        logger.info(f"High-water mark: cuma load event_ts > {last_loaded_ts}")
    else:
        logger.info("Belum ada data di staging — load seluruh bronze history (first run).")

    where_clause = ("WHERE " + " AND ".join(filters)) if filters else ""

    logger.info(f"Reading bronze parquet from {BRONZE_GLOB} ...")
    raw_count = retry_query(
        con, f"SELECT count(*) FROM read_parquet('{BRONZE_GLOB}') {where_clause}", params
    ).fetchone()[0]
    logger.info(f"Read {raw_count} raw rows.")

    if raw_count == 0:
        logger.info("Gak ada row baru sejak load terakhir — skip.")
        con.close()
        return
    dedup_key_cols = ", ".join(DEDUP_KEYS)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE bronze_dedup AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *,
                   row_number() OVER (PARTITION BY {dedup_key_cols} ORDER BY event_ts DESC) AS rn
            FROM read_parquet('{BRONZE_GLOB}')
            {where_clause}
        )
        WHERE rn = 1
    """, params)
    local_count = con.execute("SELECT count(*) FROM bronze_dedup").fetchone()[0]
    logger.info(f"Deduped locally: {local_count} rows ready to push to MotherDuck.")

    con.execute("INSERT INTO md.staging.stg_bus_arrival_raw BY NAME SELECT * FROM bronze_dedup")

    inserted = con.execute("SELECT count(*) FROM md.staging.stg_bus_arrival_raw").fetchone()[0]
    logger.info(f"Done. staging.stg_bus_arrival_raw now has {inserted} total rows.")

    con.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", help="Backfill override: force reload from this date (YYYY-MM-DD), ignoring the high-water mark")
    args = parser.parse_args()

    if not MOTHERDUCK_TOKEN:
        logger.error("MOTHERDUCK_TOKEN tidak ditemukan di .env — jalankan motherduck/setup_motherduck.py dulu.")
        return

    load(args.since)


if __name__ == "__main__":
    main()