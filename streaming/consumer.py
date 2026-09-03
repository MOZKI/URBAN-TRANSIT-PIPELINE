"""
PySpark Structured Streaming Consumer (Containerized/Internal)
Redpanda (lta.bus-arrival.raw) -> parse & enrich -> MinIO Bronze
"""
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bus-arrival-consumer")

NEXT_BUS_SCHEMA = StructType([
    StructField("OriginCode", StringType()),
    StructField("DestinationCode", StringType()),
    StructField("EstimatedArrival", StringType()),
    StructField("Monitored", IntegerType()),
    StructField("Latitude", StringType()),
    StructField("Longitude", StringType()),
    StructField("VisitNumber", StringType()),
    StructField("Load", StringType()),
    StructField("Feature", StringType()),
    StructField("Type", StringType()),
])

SERVICE_SCHEMA = StructType([
    StructField("ServiceNo", StringType()),
    StructField("Operator", StringType()),
    StructField("NextBus", NEXT_BUS_SCHEMA),
    StructField("NextBus2", NEXT_BUS_SCHEMA),
    StructField("NextBus3", NEXT_BUS_SCHEMA),
])

EVENT_SCHEMA = StructType([
    StructField("ingested_at", StringType()),
    StructField("bus_stop_code", StringType()),
    StructField("payload", StructType([
        StructField("BusStopCode", StringType()),
        StructField("Services", ArrayType(SERVICE_SCHEMA)),
    ])),
])

def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("lta-bus-arrival-consumer")
        .config("spark.jars.packages", ",".join(config.SPARK_PACKAGES))
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .config("spark.hadoop.fs.s3a.connection.keepalive", "false")
        .config("spark.hadoop.fs.s3a.connection.ttl", "5000")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def load_direction_lookup(spark: SparkSession):
    routes = spark.read.option("multiline", "true").json(config.REFERENCE_ROUTES_PATH)
    window = Window.partitionBy("service_no", "bus_stop_code").orderBy("stop_sequence")
    rows = (
        routes.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .select("service_no", "bus_stop_code", "direction")
        .collect()
    )
    lookup_dict = {(r.service_no, r.bus_stop_code): r.direction for r in rows}
    return spark.sparkContext.broadcast(lookup_dict)

def load_services_lookup(spark: SparkSession):
    services = spark.read.option("multiline", "true").json(config.REFERENCE_SERVICES_PATH)

    for freq_col in ["am_peak_freq", "am_offpeak_freq", "pm_peak_freq", "pm_offpeak_freq"]:
        parts = F.split(F.col(freq_col), "-")
        services = (
            services
            .withColumn(f"{freq_col}_low", parts.getItem(0).cast("double"))
            .withColumn(f"{freq_col}_high", parts.getItem(1).cast("double"))
        )

    return services.select(
        "service_no", "direction",
        "am_peak_freq_low", "am_peak_freq_high",
        "am_offpeak_freq_low", "am_offpeak_freq_high",
        "pm_peak_freq_low", "pm_peak_freq_high",
        "pm_offpeak_freq_low", "pm_offpeak_freq_high",
    )

def determine_period_bucket(hhmm_col):
    return (
        F.when((hhmm_col >= config.AM_PEAK_START) & (hhmm_col < config.AM_PEAK_END), F.lit("am_peak"))
        .when((hhmm_col >= config.AM_PEAK_END) & (hhmm_col < config.PM_PEAK_START), F.lit("am_offpeak"))
        .when((hhmm_col >= config.PM_PEAK_START) & (hhmm_col < config.PM_PEAK_END), F.lit("pm_peak"))
        .otherwise(F.lit("pm_offpeak"))
    )

def run():
    logger.info("Step 1/6: Building Spark session...")
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Step 1/6: DONE — Spark session ready.")

    logger.info("Step 2/6: Loading direction lookup from MinIO (bus_routes_reference.json)...")
    direction_lookup_bc = load_direction_lookup(spark)
    logger.info(f"Step 2/6: DONE — {len(direction_lookup_bc.value)} direction lookup rows loaded.")

    @F.udf(returnType=IntegerType())
    def resolve_direction(service_no, bus_stop_code):
        return direction_lookup_bc.value.get((service_no, bus_stop_code))

    logger.info("Step 3/6: Loading services lookup from MinIO (bus_services_reference.json)...")
    services_lookup = load_services_lookup(spark)
    services_lookup.cache()
    logger.info(f"Step 3/6: DONE — {services_lookup.count()} services lookup rows loaded.")

    logger.info("Step 4/6: Connecting to Kafka/Redpanda stream...")
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    logger.info("Step 4/6: DONE — Kafka stream configured (belum tentu ada data masuk).")

    logger.info("Step 5/6: Building transformation pipeline...")
    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("data"))
        .select(
            F.col("data.ingested_at").alias("ingested_at"),
            F.col("data.bus_stop_code").alias("bus_stop_code"),
            F.explode("data.payload.Services").alias("service"),
        )
    )

    filtered = parsed.filter(F.col("service.ServiceNo").isin(config.TARGET_SERVICES))

    enriched = filtered.select(
        "ingested_at",
        "bus_stop_code",
        F.col("service.ServiceNo").alias("service_no"),
        F.col("service.Operator").alias("operator"),
        F.col("service.NextBus.OriginCode").alias("origin_code"),
        F.col("service.NextBus.DestinationCode").alias("destination_code"),
        F.col("service.NextBus.EstimatedArrival").alias("next_bus_eta"),
        F.col("service.NextBus.Load").alias("next_bus_load"),
        F.col("service.NextBus2.EstimatedArrival").alias("next_bus2_eta"),
    ).filter(
        F.col("next_bus_eta").isNotNull() & (F.col("next_bus_eta") != "")
        & F.col("next_bus2_eta").isNotNull() & (F.col("next_bus2_eta") != "")
    )

    with_direction = enriched.withColumn(
        "direction", resolve_direction(F.col("service_no"), F.col("bus_stop_code"))
    )

    with_freq = with_direction.join(
        F.broadcast(services_lookup),
        on=["service_no", "direction"],
        how="left",
    )

    hhmm = F.substring(F.col("next_bus_eta"), 12, 5)
    with_period = with_freq.withColumn("period_bucket", determine_period_bucket(hhmm))

    expected_high = (
        F.when(F.col("period_bucket") == "am_peak", F.col("am_peak_freq_high"))
        .when(F.col("period_bucket") == "am_offpeak", F.col("am_offpeak_freq_high"))
        .when(F.col("period_bucket") == "pm_peak", F.col("pm_peak_freq_high"))
        .otherwise(F.col("pm_offpeak_freq_high"))
    )

    ts_pattern = "yyyy-MM-dd'T'HH:mm:ssXXX"
    result = (
        with_period.withColumn("expected_high_minutes", expected_high)
        .withColumn(
            "headway_actual_seconds",
            F.unix_timestamp(F.to_timestamp("next_bus2_eta", ts_pattern))
            - F.unix_timestamp(F.to_timestamp("next_bus_eta", ts_pattern)),
        )
        .withColumn(
            "headway_gap_seconds",
            F.col("headway_actual_seconds") - (F.col("expected_high_minutes") * 60),
        )
        .withColumn("event_ts", F.to_timestamp("ingested_at"))
    )

    deduped = (
        result.withWatermark("event_ts", config.WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark(["bus_stop_code", "service_no", "next_bus_eta"])
    )

    final_df = (
        deduped
        .withColumn("year", F.date_format("event_ts", "yyyy"))
        .withColumn("month", F.date_format("event_ts", "MM"))
        .withColumn("day", F.date_format("event_ts", "dd"))
        .withColumn("hour", F.date_format("event_ts", "HH"))
    )

    logger.info(f"Step 6/6: Starting stream: {config.KAFKA_TOPIC} -> {config.BRONZE_OUTPUT_PATH}")

    query = (
        final_df.writeStream.format("parquet")
        .option("path", config.BRONZE_OUTPUT_PATH)
        .option("checkpointLocation", config.CHECKPOINT_PATH)
        .partitionBy("year", "month", "day", "hour")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Step 6/6: DONE — Query started, waiting for micro-batches...")
    query.awaitTermination()


if __name__ == "__main__":
    run()