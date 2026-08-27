"""
PySpark Structured Streaming Consumer
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
        .config("spark.sql.shuffle.partitions", "4")  
        .getOrCreate()
    )


def load_direction_lookup(spark: SparkSession):
    routes = spark.read.json(config.REFERENCE_ROUTES_PATH)

    origin_lookup = (
        routes.filter(F.col("stop_sequence") == 1)
        .select("service_no", "direction", F.col("bus_stop_code").alias("origin_code"))
    )

    max_seq = routes.groupBy("service_no", "direction").agg(F.max("stop_sequence").alias("max_seq"))
    destination_lookup = (
        routes.join(max_seq, ["service_no", "direction"])
        .filter(F.col("stop_sequence") == F.col("max_seq"))
        .select("service_no", "direction", F.col("bus_stop_code").alias("destination_code"))
    )

    return origin_lookup.join(destination_lookup, ["service_no", "direction"])


def load_services_lookup(spark: SparkSession):
    services = spark.read.json(config.REFERENCE_SERVICES_PATH)

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
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    direction_lookup = load_direction_lookup(spark)
    services_lookup = load_services_lookup(spark)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

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

    with_direction = enriched.join(
        F.broadcast(direction_lookup),
        on=["service_no", "origin_code", "destination_code"],
        how="left",
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

    logger.info(f"Starting stream: {config.KAFKA_TOPIC} -> {config.BRONZE_OUTPUT_PATH}")

    query = (
        final_df.writeStream.format("parquet")
        .option("path", config.BRONZE_OUTPUT_PATH)
        .option("checkpointLocation", config.CHECKPOINT_PATH)
        .partitionBy("year", "month", "day", "hour")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()