"""
Unit Tests — SDP Pipeline Transformations
==========================================
Tests core transformation logic using PySpark in local mode.
No Databricks environment required to run these tests.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, BooleanType, DoubleType
)
from datetime import datetime


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
            .master("local[*]")
            .appName("sdp-unit-tests")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Business Partner — Deduplication Logic
# ---------------------------------------------------------------------------
class TestBusinessPartnerDeduplication:

    def test_dedup_keeps_latest_offset(self, spark):
        """Given multiple rows for the same BP, only the highest kafka_offset survives."""
        schema = StructType([
            StructField("sap_client",    StringType(),    True),
            StructField("bp_number",     StringType(),    True),
            StructField("event_type",    StringType(),    True),
            StructField("kafka_offset",  StringType(),    True),
            StructField("full_name",     StringType(),    True),
        ])

        data = [
            ("100", "BP001", "UPDATE", "1000", "Old Name"),
            ("100", "BP001", "UPDATE", "2000", "New Name"),  # ← should survive
            ("100", "BP002", "INSERT", "1500", "Other BP"),
        ]

        df = spark.createDataFrame(data, schema)

        from pyspark.sql.window import Window
        window = (
            Window
            .partitionBy("sap_client", "bp_number", "event_type")
            .orderBy(F.col("kafka_offset").cast("long").desc())
        )

        result = (
            df
            .withColumn("_rank", F.row_number().over(window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )

        assert result.count() == 2

        bp001 = result.filter(F.col("bp_number") == "BP001").collect()[0]
        assert bp001["full_name"] == "New Name"
        assert bp001["kafka_offset"] == "2000"

    def test_different_event_types_not_deduped(self, spark):
        """INSERT and UPDATE for the same BP should coexist after deduplication."""
        schema = StructType([
            StructField("sap_client",   StringType(), True),
            StructField("bp_number",    StringType(), True),
            StructField("event_type",   StringType(), True),
            StructField("kafka_offset", StringType(), True),
        ])

        data = [
            ("100", "BP001", "INSERT", "1000"),
            ("100", "BP001", "UPDATE", "2000"),
        ]

        df = spark.createDataFrame(data, schema)

        from pyspark.sql.window import Window
        window = (
            Window
            .partitionBy("sap_client", "bp_number", "event_type")
            .orderBy(F.col("kafka_offset").cast("long").desc())
        )

        result = (
            df
            .withColumn("_rank", F.row_number().over(window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )

        # Both INSERT and UPDATE survive because they have different event_types
        assert result.count() == 2


# ---------------------------------------------------------------------------
# Event Streaming — Aggregation Logic
# ---------------------------------------------------------------------------
class TestEventAggregations:

    def test_hourly_event_count(self, spark):
        """Hourly aggregation correctly counts events per type."""
        schema = StructType([
            StructField("event_id",        StringType(),    True),
            StructField("event_type",      StringType(),    True),
            StructField("entity_id",       StringType(),    True),
            StructField("source_system",   StringType(),    True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("amount_usd",      DoubleType(),    True),
        ])

        ts = datetime(2025, 1, 15, 10, 30, 0)  # 10:30 AM

        data = [
            ("e1", "PAGE_VIEW", "u1", "web", ts, None),
            ("e2", "PAGE_VIEW", "u2", "web", ts, None),
            ("e3", "PURCHASE",  "u1", "web", ts, 99.99),
            ("e4", "PURCHASE",  "u3", "app", ts, 49.00),
        ]

        df = spark.createDataFrame(data, schema)

        result = (
            df
            .groupBy("event_type", "source_system")
            .agg(
                F.count("*").alias("event_count"),
                F.countDistinct("entity_id").alias("unique_entities"),
                F.sum("amount_usd").alias("total_amount_usd"),
            )
        )

        page_view_web = result.filter(
            (F.col("event_type") == "PAGE_VIEW") & (F.col("source_system") == "web")
        ).collect()[0]

        assert page_view_web["event_count"] == 2
        assert page_view_web["unique_entities"] == 2
        assert page_view_web["total_amount_usd"] is None

        purchase_web = result.filter(
            (F.col("event_type") == "PURCHASE") & (F.col("source_system") == "web")
        ).collect()[0]

        assert purchase_web["event_count"] == 1
        assert abs(purchase_web["total_amount_usd"] - 99.99) < 0.01

    def test_event_deduplication_by_id(self, spark):
        """dropDuplicates on event_id correctly removes duplicate events."""
        schema = StructType([
            StructField("event_id",   StringType(), True),
            StructField("event_type", StringType(), True),
        ])

        data = [
            ("e1", "PAGE_VIEW"),
            ("e1", "PAGE_VIEW"),  # duplicate
            ("e2", "CLICK"),
        ]

        df = spark.createDataFrame(data, schema)
        result = df.dropDuplicates(["event_id"])

        assert result.count() == 2


# ---------------------------------------------------------------------------
# Data Quality — Expectation Logic
# ---------------------------------------------------------------------------
class TestDataQualityExpectations:

    def test_country_code_length_validation(self, spark):
        """Country codes must be exactly 2 characters."""
        schema = StructType([
            StructField("bp_number",    StringType(), True),
            StructField("country_code", StringType(), True),
        ])

        data = [
            ("BP001", "BR"),    # valid
            ("BP002", "USA"),   # invalid — 3 chars
            ("BP003", None),    # null — considered valid (nullable field)
            ("BP004", "DE"),    # valid
        ]

        df = spark.createDataFrame(data, schema)
        valid = df.filter("country_code IS NULL OR LENGTH(country_code) = 2")

        assert valid.count() == 3  # BP001, BP003, BP004

    def test_bp_category_valid_values(self, spark):
        """BP category must be 1, 2, or 3 when not null."""
        schema = StructType([
            StructField("bp_number",   StringType(), True),
            StructField("bp_category", StringType(), True),
        ])

        data = [
            ("BP001", "1"),
            ("BP002", "2"),
            ("BP003", "3"),
            ("BP004", "4"),    # invalid
            ("BP005", None),   # valid (nullable)
        ]

        df = spark.createDataFrame(data, schema)
        valid = df.filter("bp_category IN ('1', '2', '3') OR bp_category IS NULL")

        assert valid.count() == 4  # BP001–003 + BP005
