# Near-Realtime Urban Transit Pipeline (Singapore LTA API)

Near-realtime, event-driven data pipeline for **Service 190, 147, 2** corridor bottleneck & headway-reliability analytics, built on a hybrid speed-layer + batch-orchestration architecture.

<p align="center">
  <img width="1600" height="900" alt="Frame 23" src="https://github.com/user-attachments/assets/5cbd5a71-f2f7-4c69-a582-1b19286b933d" />
</p>


## 1. Prerequisites

- Docker + Docker Compose
- Python 3.11 (for the producer and the one-off reference scripts, run on the host)
- An LTA DataMall account key ([register here](https://datamall.lta.gov.sg/content/datamall/en/request-for-api.html))
- A MotherDuck account + token ([motherduck.com](https://motherduck.com), free tier)
- `mc` (MinIO Client) or the MinIO web console, for one manual upload step

## 2. Configure environment

```bash
cp .env.example .env
```

Fill in at minimum:

| Variable                | Notes                                 |
| ----------------------- | ------------------------------------- |
| `LTA_ACCOUNT_KEY`       | from LTA DataMall                     |
| `MOTHERDUCK_TOKEN`      | from MotherDuck                       |
| `MOTHERDUCK_DATABASE`   | defaults to `urban_transit`           |
| `POLL_MODE`             | keep `corridor`                       |
| `BUS_SERVICE_NOS`       | defaults to `190,147,2`               |
| `POLL_INTERVAL_SECONDS` | `120` (see Notes below on why not 60) |

MinIO/Redpanda credentials can stay as the sample defaults for local dev.

## 3. Provision MotherDuck (one-off)

```bash
cd motherduck
pip install -r requirements.txt
python setup_motherduck.py
cd ..
```

Creates the `urban_transit` database plus `staging` and `gold` schemas, and the four `*_raw` staging tables.

## 4. Fetch reference data (one-off)

Run in this exact order — `fetch_bus_stops.py` depends on the stop codes discovered by `fetch_bus_routes.py`:

```bash
cd scripts
pip install -r requirements.txt
python fetch_bus_services.py   # → bus_services_reference.json
python fetch_bus_routes.py     # → bus_routes_reference.json
python fetch_bus_stops.py      # → bus_stops_reference.json
python load_reference_to_staging.py   # loads all 3 JSONs into MotherDuck staging
cd ..
```

## 5. Start the always-on stack

```bash
docker compose up -d --build
```

This brings up: `redpanda`, `redpanda-console`, `minio-urban-transit`, `spark-consumer`, `postgres-airflow-utp`, `airflow-init-utp` → `airflow-webserver-utp` / `airflow-scheduler-utp`, and `metabase`.

The **producer is intentionally not containerized** (host networking made local dev on macOS simpler than port-forwarding into the Docker network) — you run it separately in step 7.

## 6. Create the MinIO bucket and upload reference lookups

The Spark consumer resolves `direction` from reference JSON it reads directly off MinIO, so this manual step has to happen before you start the producer:

1. Open the MinIO console at `http://localhost:9001` (login with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`).
2. Create a bucket named `urban-transit`.
3. Upload `scripts/bus_services_reference.json` and `scripts/bus_routes_reference.json` into `urban-transit/reference/`.

(Or via `mc`: `mc cp scripts/bus_services_reference.json scripts/bus_routes_reference.json local/urban-transit/reference/`.)

## 7. Start the producer

```bash
cd producer
pip install -r requirements.txt
python main.py
cd ..
```

Polls `/v3/BusArrival` for every stop on the Service 190/147/2 corridors every `POLL_INTERVAL_SECONDS`, staggered per request, and publishes to Redpanda.

## 8. Verify the flow

- **Redpanda Console** — `http://localhost:8080` → topic `lta.bus-arrival.raw` should show incoming messages.
- **MinIO Console** — `http://localhost:9001` → `urban-transit/bronze/bus_arrival/...` should start filling with Parquet files after a couple of polling cycles.
- **Airflow** — `http://localhost:8081` (`admin` / `admin`) → unpause `urban_transit_batch_pipeline`. It runs every 2 minutes: `load_bronze_to_staging` → `dbt_run` → `dbt_snapshot` → `dbt_test`.
- **Metabase** — `http://localhost:3000` → first-run setup, add a database connection using the DuckDB/MotherDuck driver with your `MOTHERDUCK_TOKEN`, then point dashboards at the `gold` schema (`fct_bus_delays`, `dim_bus_stops`, `dim_bus_services`).

## 9. Shutting down / resetting

```bash
docker compose down
docker compose down -v
```

Stop the producer with `Ctrl+C` separately — it's not managed by Compose.

## Known operational notes

- If LTA changes stops on Service 190/147/2, re-run `fetch_bus_routes.py` + `fetch_bus_stops.py`, re-upload the JSON to MinIO, and restart `spark-consumer` — the reference lookups are a static snapshot, not auto-refreshing.
- `POLL_INTERVAL_SECONDS=120` and `REQUEST_STAGGER_MS=120` are set to keep each polling cycle from overrunning the interval — do not drop back to `60` without re-checking cycle time.
- Airflow's `dbt_run` / `dbt_snapshot` / `dbt_test` tasks each do `rm -rf target &&` first — this is deliberate, to avoid a corrupted `dbt/target/` from an interrupted run breaking the next one.
