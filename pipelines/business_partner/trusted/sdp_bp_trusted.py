"""
TRUSTED Layer — Business Partner Pipeline
==========================================
Applies schema, deduplication, and data quality expectations.
Records that fail expectations are KEPT (tracked, not dropped).

Catalog:  <catalog>.trusted
Table:    bp_trusted
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType, BooleanType
)

# ---------------------------------------------------------------------------
# Schema — enforced at the Trusted layer (schema-on-read from RAW)
# ---------------------------------------------------------------------------
BP_SCHEMA = StructType([
    StructField("bp_number",        StringType(),    nullable=False),
    StructField("bp_category",      StringType(),    nullable=True),   # 1=Person, 2=Org, 3=Group
    StructField("full_name",        StringType(),    nullable=True),
    StructField("first_name",       StringType(),    nullable=True),
    StructField("last_name",        StringType(),    nullable=True),
    StructField("country_code",     StringType(),    nullable=True),
    StructField("region",           StringType(),    nullable=True),
    StructField("tax_number",       StringType(),    nullable=True),
    StructField("created_date",     StringType(),    nullable=True),
    StructField("changed_date",     StringType(),    nullable=True),
    StructField("is_active",        BooleanType(),   nullable=True),
    StructField("source_table",     StringType(),    nullable=True),   # KNA1, LFA1, etc.
    StructField("sap_client",       StringType(),    nullable=True),
    StructField("event_type",       StringType(),    nullable=True),   # INSERT, UPDATE, DELETE
])

# ---------------------------------------------------------------------------
# Data Quality expectations — violations tracked, records retained
# ---------------------------------------------------------------------------
BP_EXPECTATIONS = {
    "bp_number_not_null":      "bp_number IS NOT NULL",
    "bp_category_valid":       "bp_category IN ('1', '2', '3') OR bp_category IS NULL",
    "country_code_length":     "country_code IS NULL OR LENGTH(country_code) = 2",
    "event_type_valid":        "event_type IN ('INSERT', 'UPDATE', 'DELETE')",
    "sap_client_not_null":     "sap_client IS NOT NULL",
}

# ---------------------------------------------------------------------------
# TRUSTED TABLE
# ---------------------------------------------------------------------------
@dlt.table(
    name="bp_trusted",
    comment="Cleansed and validated Business Partner records — Silver layer",
    table_properties={
        "quality":                        "silver",
        "delta.enableChangeDataFeed":     "true",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_all(BP_EXPECTATIONS)  # Violations tracked in event log, records NOT dropped
def bp_trusted():
    """
    1. Parse raw JSON payload using enforced schema
    2. Deduplicate by (sap_client, bp_number, event_type) — keep latest offset
    3. Add processing metadata columns
    """
    window = (
        Window
        .partitionBy("sap_client", "bp_number", "event_type")
        .orderBy(F.col("kafka_offset").desc())
    )

    return (
        dlt.read_stream("bp_raw_kafka")
            # Parse JSON payload with schema enforcement
            .withColumn(
                "parsed",
                F.from_json(F.col("raw_payload"), BP_SCHEMA)
            )
            .select(
                "kafka_offset",
                "kafka_ingest_timestamp",
                "pipeline_ingested_at",
                "parsed.*",
            )
            # Deduplication: keep latest record per business key
            .withColumn("_row_rank", F.row_number().over(window))
            .filter(F.col("_row_rank") == 1)
            .drop("_row_rank")
            # Processing metadata
            .withColumn("trusted_processed_at", F.current_timestamp())
            .withColumn(
                "created_date_parsed",
                F.to_date(F.col("created_date"), "yyyyMMdd")
            )
            .withColumn(
                "changed_date_parsed",
                F.to_date(F.col("changed_date"), "yyyyMMdd")
            )
    )
