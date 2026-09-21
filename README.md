# Near-Realtime Urban Transit Pipeline

[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Redpanda](https://img.shields.io/badge/Redpanda-E1361A?style=for-the-badge&logo=apachekafka&logoColor=white)](https://redpanda.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-C72C48?style=for-the-badge&logo=minio&logoColor=white)](https://min.io/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/MotherDuck-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://motherduck.com/)
[![Metabase](https://img.shields.io/badge/Metabase-506477?style=for-the-badge&logo=metabase&logoColor=white)](https://www.metabase.com/)

A near-realtime, event-driven data pipeline that streams bus GPS/ETA data from the Singapore Land Transport Authority (LTA) DataMall API, computes headway-based delay metrics, and surfaces corridor bottlenecks on a near-live dashboard.

## Table of Contents
- [Background & Goal](#background--goal)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Modeling (Medallion Architecture)](#data-modeling-medallion-architecture)
- [How to Run](#how-to-run)
- [Dashboard](#dashboard)
- [Key Design Decisions](#key-design-decisions)
- [Challenges & Troubleshooting](#challenges--troubleshooting)
- [Limitations & Future Work](#limitations--future-work)
- [Author](#author)

## Background & Goal

City planners often lack real-time and historical visibility into bus delay levels and corridor bottlenecks, since static published timetables rarely reflect what's actually happening on the road. This project builds a near-realtime pipeline that:
1. Continuously polls GPS and Estimated Time of Arrival (ETA) data from the LTA API and streams it through the ingestion pipeline in near-real time.
2. Computes a headway gap score: the difference between actual bus-to-bus arrival intervals and the officially published dispatch frequency.
3. Archives every raw event to a data lake, then surfaces near-realtime and historical analysis in a warehouse and dashboard.

**Scope**: 3 representative bus services (190, 147, 2), chosen to cover distinct corridor types (express/commuter rush, urban trunk/mixed zone, dense-stop artery) while keeping corridor-bottleneck analysis coherent without polling all 5,000+ bus stops in Singapore.

**Success metrics**:
- **Latency & freshness**: data ingested and processed on a consistent ~120s polling cycle per corridor scope.
- **Headway Reliability Index**: % of bus arrival intervals within the officially published frequency range vs. exceeding it, broken down by stop, route, and time of day.
- **Bottleneck identification**: surface at least 3 stops/corridors with the highest headway gap during the observation window.
- **Data quality SLA**: 100% of ingested data passes schema integrity and null checks at staging and gold, enforced via `dbt test`.

## Architecture
<p align="center">
  <img width="1600" height="900" alt="Frame 23" src="https://github.com/user-attachments/assets/53b11e6d-d651-4626-a5a6-64ed68555486" />
</p>

The pipeline is a **hybrid architecture**: an always-on speed layer for ingestion, decoupled from a scheduled batch layer for warehousing.

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Data Source | Singapore LTA DataMall API | Official, free, granular update frequency (20s for BusArrival) |
| Ingestion | Python (`requests`) | Simple polling for a REST API with no push mechanism |
| Message Broker | Redpanda | Kafka wire-compatible, lighter footprint for local dev |
| Stream Processor | PySpark Structured Streaming (containerized) | Stateful, watermarked dedup for correctness on out-of-order/retried events |
| Raw Storage | MinIO | S3-compatible, immutable Bronze layer for reprocessing |
| Data Warehouse | MotherDuck (managed cloud DuckDB) | Permanent free tier, OLAP columnar engine, native `MERGE`/`dbt snapshot` support |
| Transformation & DQ | dbt Core (`dbt-duckdb`) | Industry standard, built-in testing & SCD2 snapshots |
| Orchestration | Apache Airflow (LocalExecutor + Postgres) | Retry, monitoring, dependency management for the batch layer |
| Visualization | Metabase (self-hosted) | Free, open-source, official DuckDB/MotherDuck driver |

## Project Structure

```
urban-transit-pipeline/
├── producer/
│   ├── main.py                        # LTA API poller -> Redpanda producer
│   └── requirements.txt
├── streaming/
│   ├── consumer.py                    # PySpark Structured Streaming job
│   ├── config.py
│   ├── Dockerfile
│   └── requirements.txt
├── scripts/
│   ├── fetch_bus_services.py          # one-off: published dispatch frequency
│   ├── fetch_bus_routes.py            # one-off: route <-> stop resolution
│   ├── fetch_bus_stops.py             # one-off: stop master data
│   ├── load_reference_to_staging.py   # loads reference JSON -> MotherDuck staging
│   └── load_bronze_to_staging.py      # MinIO Bronze -> MotherDuck staging (high-water-mark incremental)
├── motherduck/
│   └── setup_motherduck.py            # one-off: provision database/schemas
├── airflow/
│   ├── dags/urban_transit_batch_pipeline.py
│   └── Dockerfile
├── dbt/
│   ├── models/
│   │   ├── staging/                   # cleansing, casting, dedup
│   │   └── gold/                      # fct_bus_delays, dim_bus_stops, dim_bus_services
│   ├── snapshots/dim_bus_stops_snapshot.sql   # SCD2
│   └── dbt_project.yml
├── metabase/
│   └── Dockerfile                     # custom build: Metabase + MotherDuck driver
├── docker-compose.yml
├── .env.example
└── README.md
```

## Data Modeling (Medallion Architecture)

- **Bronze (MinIO)**: raw JSON converted to Parquet, partitioned by `year/month/day/hour`.
- **Staging (MotherDuck)**: schema cleanup, ISO timestamp casting, GPS coordinate normalization, null handling.
- **Gold (MotherDuck) — star schema**:
  - `fct_bus_delays`: headway gap seconds, bus stop, service number, direction, window timestamp.
  - `dim_bus_stops`: stop location & road name, keyed on the composite `(service_no, direction, bus_stop_code)` — a stop is tied to its specific route and direction, not just its code. Historized via `dbt snapshot` (SCD Type 2, `effective_since`).
  - `dim_bus_services`: published dispatch frequency per route per hour, the baseline used for headway comparison.

## How to Run

### 1. Prerequisites
- Docker & Docker Compose
- Python 3.11
- A free [LTA DataMall](https://datamall.lta.gov.sg/) API key
- A free [MotherDuck](https://motherduck.com/) account + token

### 2. Setup
```bash
git clone https://github.com/<your-username>/urban-transit-pipeline.git
cd urban-transit-pipeline
cp .env.example .env
```
Fill in `.env` with your `LTA_ACCOUNT_KEY` and `MOTHERDUCK_TOKEN`.

### 3. Provision the warehouse (one-off)
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r motherduck/requirements.txt
python motherduck/setup_motherduck.py
```

### 4. Fetch reference data (one-off)
```bash
pip install -r scripts/requirements.txt
python scripts/fetch_bus_services.py
python scripts/fetch_bus_routes.py
python scripts/fetch_bus_stops.py
python scripts/load_reference_to_staging.py
```

### 5. Start the core services
```bash
docker compose up -d
```
This starts Redpanda, Redpanda Console, MinIO, the Spark consumer, Postgres + Airflow (webserver/scheduler), and Metabase.

- Create an `urban-transit` bucket in the MinIO Console (`localhost:9001`) with a `reference/` prefix, and upload the 3 reference JSON files there (used by the Spark consumer for direction lookups).

### 6. Start the producer
The producer runs as a local process (not containerized) to avoid Docker Desktop port-forwarding quirks on macOS:
```bash
pip install -r producer/requirements.txt
python producer/main.py
```

### 7. Let it run
The Spark consumer continuously writes events to MinIO Bronze. The Airflow DAG (`urban_transit_batch_pipeline`, every 2 minutes) automatically runs `load_bronze_to_staging → dbt run → dbt snapshot → dbt test`.

### 8. Access the services
| Service | URL |
|---|---|
| Redpanda Console | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Airflow | http://localhost:8081 (`admin` / `admin`) |
| Metabase | http://localhost:3000 |

## Dashboard

<!-- TODO: add a dashboard screenshot if available, otherwise remove this image block -->
<p align="center">
  <img width="1688" height="2137" alt="UTP-DASHBOARD" src="https://github.com/user-attachments/assets/29081b7b-b93a-4974-8459-9744bf4f5477" />
</p>

The Metabase dashboard (Corridor 190, 147, 2) includes:
- Real-Time Corridor Bottleneck Heatmap
- Headway Reliability Index
- Average Headway Gap Trend by peak hours (07:00–09:00 & 17:00–19:00 SST)
- Bus Occupancy vs. Congestion correlation
- Data Freshness indicator (last processed event, total records)

## Key Design Decisions

- **Hybrid speed + batch layer**: ingestion runs always-on and independently of the scheduled warehouse/transform layer, so real-time ingestion is never blocked by batch job duration.
- **Spark over plain Python**: at this project's scale, a plain Python consumer could already handle dedup/windowing manually. Spark was chosen for production-readiness and easier scale-up if the corridor scope grows, not because the current volume needs distributed compute.

## Challenges & Troubleshooting

- **Redpanda Console `Backend Error: connection refused`** on the Topics page, despite the producer sending data successfully → caused by a single listener only exposing `localhost:9092`; fixed with a proper dual-listener setup (internal `redpanda:9092`, external `localhost:19092`).
- **Spark consumer wrote nothing to MinIO** even though events were landing in Redpanda → root cause was broken internal connectivity between the consumer and Redpanda/MinIO; fixed by containerizing the consumer on the same Docker network.
- **Airflow task `load_bronze_to_staging` stuck "up for retry"** → DuckDB's `httpfs` (MinIO) and `motherduck` extensions conflicted on a single connection; split into two independent connections, transferring data via Arrow.

## Trade-Offs

- Added architectural complexity (hybrid speed + batch) beyond what this use case strictly needed, as a deliberate choice to build production-style streaming experience.
- Set polling to 120s rather than the API's supported 20s refresh rate. This means the pipeline can't track per-second bus movement, but keeps resource usage low.
- Prioritized lightweight local footprint over raw performance for some tool choices (e.g., Redpanda over a heavier broker).
- Scoped to 3 corridors to keep bottleneck analysis coherent, at the cost of city-wide coverage.

## Author

**Mohammad Zaki Iskandar**
Information Systems & Technology student at Universitas Negeri Jakarta.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mzakiisk/)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white)](https://github.com/MOZKI)



