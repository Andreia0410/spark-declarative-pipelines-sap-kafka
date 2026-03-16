"""
GOLD Layer — Business Partner Pipeline
========================================
Consolidates records from multiple SAP tables (KNA1, LFA1, KNVV, etc.)
into a single, unified Business Partner JSON entity, then writes
to Kafka using Avro serialization via Confluent Schema Registry.

Catalog:  <catalog>.gold
Table:    bp_gold_unified
Output:   Kafka topic  masterdata.business_partner.v1
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP      = spark.conf.get("kafka.bootstrap.servers")
KAFKA_OUTPUT_TOPIC   = spark.conf.get("kafka.topic.bp.output", "masterdata.business_partner.v1")
SCHEMA_REGISTRY_URL  = dbutils.secrets.get("sdp-kafka-scope", "schema-registry-url")
SCHEMA_REGISTRY_KEY  = dbutils.secrets.get("sdp-kafka-scope", "schema-registry-api-key")
SCHEMA_REGISTRY_SEC  = dbutils.secrets.get("sdp-kafka-scope", "schema-registry-api-secret")
KAFKA_USERNAME       = dbutils.secrets.get("sdp-kafka-scope", "kafka-username")
KAFKA_PASSWORD       = dbutils.secrets.get("sdp-kafka-scope", "kafka-password")

SR_CONFIG = {
    f"schema.registry.url":                          SCHEMA_REGISTRY_URL,
    f"confluent.schema.registry.basic.auth.user.info": f"{SCHEMA_REGISTRY_KEY}:{SCHEMA_REGISTRY_SEC}",
    "schema.registry.basic.auth.credentials.source": "USER_INFO",
}

KAFKA_WRITE_OPTIONS = {
    "kafka.bootstrap.servers":   KAFKA_BOOTSTRAP,
    "topic":                     KAFKA_OUTPUT_TOPIC,
    "kafka.security.protocol":   "SASL_SSL",
    "kafka.sasl.mechanism":      "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
    ),
}

# ---------------------------------------------------------------------------
# GOLD TABLE — unified business partner entity
# ---------------------------------------------------------------------------
@dlt.table(
    name="bp_gold_unified",
    comment=(
        "Unified Business Partner entity — Gold layer. "
        "Consolidates all SAP source tables into a single structured record."
    ),
    table_properties={
        "quality":            "gold",
        "delta.enableChangeDataFeed": "true",
    },
)
def bp_gold_unified():
    """
    Pivots rows from multiple SAP source tables into a single record per BP.
    Each SAP table (KNA1=customer, LFA1=vendor, etc.) contributes attributes
    to the consolidated entity, joined by (sap_client, bp_number).
    """
    trusted = dlt.read("bp_trusted")

    # Pivot: one row per source_table → one column per source_table's attributes
    customer_attrs = trusted.filter(F.col("source_table") == "KNA1").select(
        "sap_client", "bp_number",
        F.col("full_name").alias("customer_name"),
        F.col("country_code").alias("customer_country"),
        F.col("region").alias("customer_region"),
    )

    vendor_attrs = trusted.filter(F.col("source_table") == "LFA1").select(
        "sap_client", "bp_number",
        F.col("full_name").alias("vendor_name"),
        F.col("tax_number").alias("vendor_tax_id"),
    )

    # Base record from the canonical BP table
    base = (
        trusted
        .filter(F.col("source_table") == "BUT000")
        .select(
            "sap_client", "bp_number", "bp_category",
            "first_name", "last_name", "is_active",
            "created_date_parsed", "changed_date_parsed",
            "trusted_processed_at",
        )
        .dropDuplicates(["sap_client", "bp_number"])
    )

    unified = (
        base
        .join(customer_attrs, on=["sap_client", "bp_number"], how="left")
        .join(vendor_attrs,   on=["sap_client", "bp_number"], how="left")
        .withColumn(
            "unified_name",
            F.coalesce(
                F.col("customer_name"),
                F.col("vendor_name"),
                F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
            )
        )
        .withColumn("gold_processed_at", F.current_timestamp())
        # Serialize entire record to JSON for Kafka
        .withColumn(
            "entity_json",
            F.to_json(F.struct(
                "sap_client", "bp_number", "bp_category",
                "unified_name", "customer_country", "customer_region",
                "vendor_tax_id", "is_active",
                "created_date_parsed", "changed_date_parsed",
                "gold_processed_at"
            ))
        )
    )

    return unified


# ---------------------------------------------------------------------------
# SINK — write Gold entity to Kafka using Avro + Schema Registry
# ---------------------------------------------------------------------------
@dlt.table(
    name="bp_sink_kafka",
    comment="Kafka sink: serializes Gold entity to Avro and writes to output topic",
    table_properties={"quality": "gold"},
)
def bp_sink_kafka():
    """
    Reads from Gold unified table and writes Avro-serialized messages to Kafka.
    Uses Confluent Schema Registry for schema evolution management.
    """
    from pyspark.sql.avro.functions import to_avro

    gold = dlt.read_stream("bp_gold_unified")

    avro_schema_str = spark.conf.get("avro.schema.business_partner")

    return (
        gold
        .select(
            # Kafka message key: partition by business partner number
            F.col("bp_number").cast(StringType()).alias("key"),
            # Avro-serialize the payload
            to_avro(
                F.struct(
                    "sap_client", "bp_number", "bp_category",
                    "unified_name", "customer_country",
                    "vendor_tax_id", "is_active", "gold_processed_at"
                ),
                avro_schema_str,
            ).alias("value"),
        )
        .writeStream
        .format("kafka")
        .options(**KAFKA_WRITE_OPTIONS)
        .option("checkpointLocation", f"/checkpoints/bp_sink_kafka")
        .trigger(availableNow=True)
        .start()
    )
