"""
RAW Layer — Business Partner Pipeline
======================================
Ingests SAP Business Partner events from Kafka as-is (no transformation).
Schema-on-read approach: raw bytes stored as string for full auditability.

Catalog:  <catalog>.raw
Table:    bp_raw_kafka
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# ---------------------------------------------------------------------------
# Configuration — resolved from Databricks secrets at runtime
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP   = spark.conf.get("kafka.bootstrap.servers")
KAFKA_TOPIC       = spark.conf.get("kafka.topic.business_partner")
KAFKA_USERNAME    = dbutils.secrets.get("sdp-kafka-scope", "kafka-username")
KAFKA_PASSWORD    = dbutils.secrets.get("sdp-kafka-scope", "kafka-password")

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers":                    KAFKA_BOOTSTRAP,
    "subscribe":                                  KAFKA_TOPIC,
    "startingOffsets":                            "latest",
    "kafka.security.protocol":                    "SASL_SSL",
    "kafka.sasl.mechanism":                       "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
    ),
    "failOnDataLoss": "false",
}

# ---------------------------------------------------------------------------
# RAW TABLE — append-only, no transformation
# ---------------------------------------------------------------------------
@dlt.table(
    name="bp_raw_kafka",
    comment="Raw Business Partner events from Kafka — Bronze layer, append-only",
    table_properties={
        "quality":              "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed":     "true",
    },
)
def bp_raw_kafka():
    """
    Reads the Kafka stream and stores raw bytes with envelope metadata.
    No schema applied here — schema enforcement happens in TRUSTED.
    """
    return (
        spark.readStream
            .format("kafka")
            .options(**KAFKA_OPTIONS)
            .load()
            .select(
                F.col("offset").cast(LongType()).alias("kafka_offset"),
                F.col("partition").cast("int").alias("kafka_partition"),
                F.col("timestamp").alias("kafka_ingest_timestamp"),
                F.col("topic").alias("kafka_topic"),
                # Store raw bytes as string — full fidelity, no data loss
                F.col("value").cast("string").alias("raw_payload"),
                F.col("key").cast("string").alias("message_key"),
                # Ingestion watermark for late-data handling in downstream layers
                F.current_timestamp().alias("pipeline_ingested_at"),
            )
    )
