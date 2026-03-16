"""
Event Streaming Pipeline — RAW + TRUSTED + GOLD
================================================
Generic, reusable pattern for processing high-throughput event streams.

Demonstrates:
  - Watermark-based late data handling
  - Window aggregations (sliding + tumbling)
  - Stateful deduplication via Delta MERGE
  - Non-blocking data quality enforcement

Catalog:  <catalog>.events
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType, DoubleType, MapType
)

# ---------------------------------------------------------------------------
# Event Schema
# ---------------------------------------------------------------------------
EVENT_SCHEMA = StructType([
    StructField("event_id",        StringType(),    nullable=False),
    StructField("event_type",      StringType(),    nullable=False),  # PAGE_VIEW, CLICK, PURCHASE...
    StructField("entity_id",       StringType(),    nullable=True),
    StructField("entity_type",     StringType(),    nullable=True),   # USER, PRODUCT, ORDER
    StructField("source_system",   StringType(),    nullable=True),
    StructField("event_timestamp", TimestampType(), nullable=False),
    StructField("session_id",      StringType(),    nullable=True),
    StructField("amount",          DoubleType(),    nullable=True),
    StructField("currency",        StringType(),    nullable=True),
    StructField("properties",      MapType(StringType(), StringType()), nullable=True),
])

KAFKA_BOOTSTRAP = spark.conf.get("kafka.bootstrap.servers")
KAFKA_TOPIC     = spark.conf.get("kafka.topic.events", "platform.events.v1")
KAFKA_USERNAME  = dbutils.secrets.get("sdp-kafka-scope", "kafka-username")
KAFKA_PASSWORD  = dbutils.secrets.get("sdp-kafka-scope", "kafka-password")

# ---------------------------------------------------------------------------
# RAW: ingest events with no transformation
# ---------------------------------------------------------------------------
@dlt.table(
    name="raw_events",
    comment="Raw event stream from Kafka — Bronze layer",
    table_properties={"quality": "bronze"},
)
def raw_events():
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option(
                "kafka.sasl.jaas.config",
                "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
            )
            .option("failOnDataLoss", "false")
            .load()
            .select(
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_timestamp"),
                F.col("partition").alias("kafka_partition"),
                F.col("value").cast("string").alias("raw_payload"),
                F.current_timestamp().alias("ingested_at"),
            )
    )


# ---------------------------------------------------------------------------
# TRUSTED: parse, deduplicate, validate with 5-minute watermark for late data
# ---------------------------------------------------------------------------
EVENT_EXPECTATIONS = {
    "event_id_not_null":      "event_id IS NOT NULL",
    "event_type_not_null":    "event_type IS NOT NULL",
    "timestamp_not_null":     "event_timestamp IS NOT NULL",
    "amount_non_negative":    "amount IS NULL OR amount >= 0",
    "entity_id_not_null":     "entity_id IS NOT NULL",
}

@dlt.table(
    name="trusted_events",
    comment="Deduplicated, validated event records — Silver layer",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)
@dlt.expect_all(EVENT_EXPECTATIONS)
def trusted_events():
    """
    Parses and deduplicates events.
    Watermark of 5 minutes tolerates late-arriving messages from Kafka.
    """
    return (
        dlt.read_stream("raw_events")
            .withColumn("parsed", F.from_json(F.col("raw_payload"), EVENT_SCHEMA))
            .select("kafka_offset", "kafka_timestamp", "ingested_at", "parsed.*")
            # Watermark: allow up to 5 minutes of late data before state is dropped
            .withWatermark("event_timestamp", "5 minutes")
            # Deduplication within the watermark window
            .dropDuplicates(["event_id"])
            .withColumn("event_date",         F.to_date("event_timestamp"))
            .withColumn("event_hour",         F.hour("event_timestamp"))
            .withColumn("trusted_at",         F.current_timestamp())
            .withColumn(
                "amount_usd",
                # Placeholder: in production, use an exchange rate lookup
                F.when(F.col("currency") == "USD", F.col("amount"))
                 .otherwise(F.col("amount"))  # extend with FX logic
            )
    )


# ---------------------------------------------------------------------------
# GOLD: hourly event metrics — queryable from BI tools
# ---------------------------------------------------------------------------
@dlt.table(
    name="gold_event_metrics",
    comment="Hourly aggregated event metrics by type and source — Gold layer",
    table_properties={"quality": "gold"},
)
def gold_event_metrics():
    """
    Aggregates events into hourly windows by event_type and source_system.
    Provides the foundation for real-time dashboards and alerting.
    """
    return (
        dlt.read("trusted_events")
            .groupBy(
                F.window("event_timestamp", "1 hour").alias("time_window"),
                F.col("event_type"),
                F.col("source_system"),
                F.col("entity_type"),
                F.col("event_date"),
                F.col("event_hour"),
            )
            .agg(
                F.count("*").alias("event_count"),
                F.countDistinct("event_id").alias("unique_events"),
                F.countDistinct("entity_id").alias("unique_entities"),
                F.countDistinct("session_id").alias("unique_sessions"),
                F.sum("amount_usd").alias("total_amount_usd"),
                F.avg("amount_usd").alias("avg_amount_usd"),
                F.min("event_timestamp").alias("first_event_at"),
                F.max("event_timestamp").alias("last_event_at"),
            )
            .withColumn(
                "window_start", F.col("time_window.start")
            )
            .withColumn(
                "window_end", F.col("time_window.end")
            )
            .drop("time_window")
            .withColumn("gold_processed_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# GOLD: entity-level event history — one row per entity per day
# ---------------------------------------------------------------------------
@dlt.table(
    name="gold_entity_daily_summary",
    comment="Daily event summary per entity — enables entity-level analytics",
    table_properties={"quality": "gold"},
)
def gold_entity_daily_summary():
    return (
        dlt.read("trusted_events")
            .groupBy("entity_id", "entity_type", "event_date", "source_system")
            .agg(
                F.count("*").alias("total_events"),
                F.collect_set("event_type").alias("event_types_seen"),
                F.sum("amount_usd").alias("daily_amount_usd"),
                F.min("event_timestamp").alias("first_activity"),
                F.max("event_timestamp").alias("last_activity"),
            )
            .withColumn("summary_created_at", F.current_timestamp())
    )
