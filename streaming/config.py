"""
PySpark Structured Streaming consumer configurations
"""
import os

from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, "..", ".env"))

# kafka / redpanda
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("REDPANDA_BROKER", "redpanda:9092")
KAFKA_TOPIC = os.environ.get("TOPIC_NAME", "lta.bus-arrival.raw")

# minio / S3
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio-urban-transit:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "urban-transit")

REFERENCE_SERVICES_PATH = f"s3a://{MINIO_BUCKET}/reference/bus_services_reference.json"
REFERENCE_ROUTES_PATH = f"s3a://{MINIO_BUCKET}/reference/bus_routes_reference.json"

BRONZE_OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/bronze/bus_arrival"
CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/_checkpoints/bus_arrival_consumer"

TARGET_SERVICES = [s.strip() for s in os.environ.get("BUS_SERVICE_NOS", "190,147,2").split(",")]

AM_PEAK_START, AM_PEAK_END = "06:30", "08:30"
PM_PEAK_START, PM_PEAK_END = "17:00", "19:00"

# watermark delay
WATERMARK_DELAY = "2 minutes"

SPARK_PACKAGES = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]