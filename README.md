# ⚡ Spark Declarative Pipelines

<div align="center">

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-FFFFFF?style=for-the-badge&logo=apachespark&logoColor=#E35A16)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=delta&logoColor=white)

**Production-grade event streaming pipelines using Spark Declarative Pipelines (SDP) on Databricks,**  
**following Medallion Architecture with Unity Catalog governance.**

[Architecture](#-architecture) · [Pipelines](#-pipelines) · [Event Streaming](#-event-streaming) · [Getting Started](#-getting-started) · [Data Quality](#-data-quality)

</div>

---

## 🎯 Overview

This repository showcases a **real-world data platform** built on top of Databricks **Spark Declarative Pipelines**, designed to process high-volume event streams from Apache Kafka through a full Medallion Architecture (Raw → Trusted → Sink → Gold).

The platform handles **SAP master data events** and **business partner records** at scale, applying schema validation, deduplication, data quality checks, and Avro serialization for downstream consumers.

### Key capabilities

- 🔄 **Real-time event ingestion** from Kafka with Structured Streaming  
- 🏅 **Medallion Architecture** enforced via SDP declarative tables  
- 🔐 **Unity Catalog** governance with fine-grained access control  
- 📐 **Avro + Schema Registry** for type-safe Kafka output  
- ✅ **Data Quality** monitoring with `@expect` decorators (no record drop)  
- 🏗️ **Infrastructure as Code** with Databricks Asset Bundles (DAB)

---

## 🏛️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│                                                                     │
│   ┌─────────────┐          ┌──────────────┐                        │
│   │  SAP ERP    │          │  Kafka Topic │                        │
│   │  (Events)   │─────────▶│  (Raw Events)│                        │
│   └─────────────┘          └──────┬───────┘                        │
└────────────────────────────────── │ ──────────────────────────────┘
                                    │  Structured Streaming
                ┌───────────────────▼───────────────────┐
                │         MEDALLION ARCHITECTURE         │
                │                                       │
                │  ┌──────────┐    ┌──────────┐        │
                │  │  RAW     │───▶│ TRUSTED  │        │
                │  │  Layer   │    │  Layer   │        │
                │  │ (Bronze) │    │ (Silver) │        │
                │  └──────────┘    └────┬─────┘        │
                │                       │               │
                │              ┌────────▼────────┐      │
                │              │      SINK        │      │
                │              │  (Kafka Output) │      │
                │              └────────┬────────┘      │
                │                       │               │
                │              ┌────────▼────────┐      │
                │              │      GOLD        │      │
                │              │ (Business View) │      │
                │              └─────────────────┘      │
                └───────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
             ┌──────────┐   ┌──────────┐   ┌──────────┐
             │  Power   │   │  Kafka   │   │  Unity   │
             │   BI     │   │ Consumer │   │ Catalog  │
             └──────────┘   └──────────┘   └──────────┘
```

### Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Orchestration | Databricks SDP / DLT | Declarative pipeline management |
| Streaming | Apache Kafka | Event ingestion and output |
| Storage | Delta Lake + Unity Catalog | Governed data storage |
| Serialization | Apache Avro + Schema Registry | Type-safe event contracts |
| Processing | PySpark Structured Streaming | Stateful event transformations |
| Quality | SDP `@expect` decorators | Non-blocking data quality checks |
| IaC | Databricks Asset Bundles | Pipeline deployment |

---

## 📁 Repository Structure

```
spark-declarative-pipelines/
│
├── 📂 pipelines/
│   ├── 📂 business_partner/          # SAP Business Partner pipeline
│   │   ├── raw/                      # Bronze: raw Kafka ingest
│   │   ├── trusted/                  # Silver: cleansed + validated
│   │   ├── sink/                     # Kafka output (Avro serialized)
│   │   └── gold/                     # Unified business partner entity
│   │
│   └── 📂 event_streaming/           # Generic event streaming pipeline
│       ├── raw/                      # Raw event capture
│       ├── trusted/                  # Deduplication + enrichment
│       └── gold/                     # Aggregated event analytics
│
├── 📂 schemas/
│   ├── avro/                         # Avro schemas for Kafka output
│   └── json/                         # JSON schemas for validation
│
├── 📂 notebooks/
│   ├── data_quality/                 # DQ monitoring dashboards
│   └── exploration/                  # Ad-hoc event analysis
│
├── 📂 config/
│   ├── databricks.yml                # Asset Bundle configuration
│   └── pipeline_settings.json       # Pipeline parameters
│
├── 📂 tests/                         # Unit tests for transformations
├── 📂 docs/                          # Architecture deep-dives
└── 📂 .github/workflows/             # CI/CD automation
```

---

## 🔄 Pipelines

### Business Partner Pipeline

Processes **SAP Business Partner master data events** arriving via Kafka, consolidating multiple SAP tables (KNA1, LFA1, KNVV, etc.) into a unified entity.

```
Kafka (SAP CDC) → RAW → TRUSTED → SINK (Avro/Kafka) → GOLD (Unified Entity)
```

**Key features:**
- Full + incremental refresh strategy with checkpoint management
- Schema evolution handling via Confluent Schema Registry  
- Composite deduplication key across SAP client + partner number
- Gold layer consolidates N table records into a single JSON business partner

### Event Streaming Pipeline

Generic pattern for **high-throughput event processing**, demonstrating:
- Watermark-based late data handling
- Stateful aggregations with sliding windows
- Event deduplication with Delta Lake MERGE
- Real-time metrics exposed to Gold layer

---

## 🌊 Event Streaming

The event streaming pipeline demonstrates production patterns for **processing millions of events per day** with exactly-once semantics.

### Ingestion Pattern (RAW Layer)

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="raw_events",
    comment="Raw events ingested from Kafka with no transformation",
    table_properties={"quality": "bronze"}
)
def raw_events():
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap)
            .option("subscribe", "events.topic.v1")
            .option("startingOffsets", "latest")
            .option("kafka.security.protocol", "SASL_SSL")
            .load()
            .select(
                F.col("offset"),
                F.col("timestamp").alias("kafka_timestamp"),
                F.col("partition"),
                F.from_json(
                    F.col("value").cast("string"),
                    events_schema
                ).alias("payload")
            )
            .select("offset", "kafka_timestamp", "partition", "payload.*")
    )
```

### Deduplication Pattern (TRUSTED Layer)

```python
@dlt.table(
    name="trusted_events",
    comment="Deduplicated and validated events",
    table_properties={"quality": "silver"}
)
@dlt.expect_all({
    "valid_event_id":   "event_id IS NOT NULL",
    "valid_timestamp":  "event_timestamp IS NOT NULL",
    "valid_event_type": "event_type IN ('CREATE', 'UPDATE', 'DELETE')"
})
def trusted_events():
    return (
        dlt.read_stream("raw_events")
            .dropDuplicates(["event_id"])
            .withColumn("processing_timestamp", F.current_timestamp())
            .withColumn("event_date", F.to_date("event_timestamp"))
    )
```

### Aggregation Pattern (GOLD Layer)

```python
@dlt.table(
    name="gold_event_metrics",
    comment="Real-time event aggregations by type and hour",
    table_properties={"quality": "gold"}
)
def gold_event_metrics():
    return (
        dlt.read("trusted_events")
            .groupBy(
                F.window("event_timestamp", "1 hour").alias("window"),
                F.col("event_type"),
                F.col("source_system")
            )
            .agg(
                F.count("*").alias("event_count"),
                F.countDistinct("entity_id").alias("unique_entities"),
                F.min("event_timestamp").alias("window_start"),
                F.max("event_timestamp").alias("window_end")
            )
    )
```

---

## 🚀 Getting Started

### Prerequisites

- Databricks workspace (Premium or above)
- Unity Catalog enabled
- Kafka cluster with SASL authentication
- Confluent Schema Registry (for Avro pipelines)
- Databricks CLI configured

### 1. Clone the repository

```bash
git clone https://github.com/<your-user>/spark-declarative-pipelines.git
cd spark-declarative-pipelines
```

### 2. Configure Databricks CLI

```bash
databricks configure --profile dev
databricks configure --profile prod
```

### 3. Set up secrets

```bash
# Kafka credentials
databricks secrets create-scope sdp-kafka-scope --profile dev
databricks secrets put --scope sdp-kafka-scope --key kafka-username --profile dev
databricks secrets put --scope sdp-kafka-scope --key kafka-password --profile dev

# Schema Registry credentials
databricks secrets put --scope sdp-kafka-scope --key schema-registry-url --profile dev
databricks secrets put --scope sdp-kafka-scope --key schema-registry-api-key --profile dev
```

### 4. Deploy with Asset Bundles

```bash
# Validate configuration
databricks bundle validate --target dev

# Deploy pipelines
databricks bundle deploy --target dev

# Run the pipeline
databricks bundle run business_partner_pipeline --target dev
```

---

## ✅ Data Quality

Data quality is enforced **without dropping records**, using SDP's `@expect_all()` decorator. Violations are tracked in the pipeline event log and surfaced through a monitoring notebook.

```python
BUSINESS_PARTNER_EXPECTATIONS = {
    "bp_number_not_null":    "bp_number IS NOT NULL",
    "valid_bp_category":     "bp_category IN ('1', '2', '3')",
    "country_code_valid":    "LENGTH(country_code) = 2",
    "created_date_not_null": "created_date IS NOT NULL"
}

@dlt.expect_all(BUSINESS_PARTNER_EXPECTATIONS)
def trusted_business_partner():
    ...
```

Metrics are queryable from the event log:

```sql
SELECT
    expectations.name,
    SUM(expectations.failed_records) AS failed_records,
    SUM(expectations.passed_records) AS passed_records,
    ROUND(
        SUM(expectations.passed_records) * 100.0 /
        (SUM(expectations.passed_records) + SUM(expectations.failed_records)), 2
    ) AS pass_rate_pct
FROM
    event_log('<pipeline_id>')
LATERAL VIEW EXPLODE(details:flow_progress.data_quality.expectations) t AS expectations
WHERE event_type = 'flow_progress'
GROUP BY 1
ORDER BY pass_rate_pct ASC
```

---

## 📐 Avro Schema Example

Output to Kafka is serialized using **Apache Avro** with Confluent Schema Registry for full schema evolution support.

```json
{
  "type": "record",
  "name": "BusinessPartner",
  "namespace": "com.platform.masterdata",
  "fields": [
    { "name": "bp_number",    "type": "string" },
    { "name": "bp_category",  "type": ["null", "string"], "default": null },
    { "name": "full_name",    "type": ["null", "string"], "default": null },
    { "name": "country_code", "type": ["null", "string"], "default": null },
    { "name": "updated_at",   "type": "long", "logicalType": "timestamp-millis" }
  ]
}
```

---

## 📊 Monitoring

The pipeline exposes metrics through:
- **Databricks Pipeline UI** — flow-level throughput and DQ violations  
- **Data Quality notebook** — aggregated expectations pass/fail rates  
- **Gold layer tables** — queryable from Power BI via Databricks SQL Warehouse

---

## 🤝 Contributing

Contributions are welcome! Please open an issue first to discuss proposed changes.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -m 'feat: add my feature'`)
4. Push and open a Pull Request

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">
Built with ❤️ using Databricks Spark Declarative Pipelines
</div>
