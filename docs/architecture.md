# Architecture Deep-Dive

## Medallion Architecture in SDP

This platform implements the **Medallion Architecture** natively through Spark Declarative Pipelines (SDP) — Databricks' evolution of Delta Live Tables (DLT). Each layer is a declared table with explicit lineage, quality expectations, and storage properties managed by the platform.

---

## Layer Responsibilities

### RAW (Bronze)
- **Principle:** Ingest once, never transform  
- Stores raw Kafka bytes as string — full auditability  
- Append-only, no deduplication  
- Retains `kafka_offset`, `kafka_partition`, and `kafka_timestamp` for replay  

### TRUSTED (Silver)
- **Principle:** Cleanse, validate, deduplicate  
- Applies enforced schema via `from_json()`  
- Deduplication by composite business key using `ROW_NUMBER()` window  
- Data quality via `@dlt.expect_all()` — violations tracked, records retained  
- Adds watermark for late-data tolerance in streaming mode  

### SINK (Kafka Output)
- **Principle:** Publish to downstream systems  
- Converts Gold entity to Avro using Confluent Schema Registry  
- Uses `bp_number` as Kafka message key for partition locality  
- Triggered via `availableNow=True` for micro-batch semantics  

### GOLD
- **Principle:** Serve business-ready data  
- Consolidates N source-table rows into a unified domain entity  
- Optimized for BI tools (Power BI via SQL Warehouse) and API consumers  
- Contains no raw SAP field names — domain vocabulary only  

---

## Event Streaming Design Decisions

### Why Watermarks?
Kafka consumers can receive late-arriving messages (network delays, consumer lag). Without a watermark, Spark's streaming engine retains state indefinitely. We set a **5-minute watermark** on `event_timestamp`, meaning:
- Events arriving up to 5 minutes late are correctly processed
- State for windows older than 5 minutes is discarded — bounded memory usage

### Why `dropDuplicates` at Trusted?
Kafka does not guarantee exactly-once delivery. Producers may retry, resulting in duplicate events with identical `event_id`. We apply `dropDuplicates(["event_id"])` **within the watermark window** at the Trusted layer, ensuring idempotent processing without storing unlimited state.

### Why Avro over JSON for Kafka output?
| Concern | JSON | Avro |
|---------|------|------|
| Schema enforcement | Optional | Mandatory (Schema Registry) |
| Binary size | Larger | Compact (~30–50% smaller) |
| Schema evolution | Manual | Managed (backward/forward compat) |
| Consumer contract | Implicit | Explicit — schema versioned |

---

## Unity Catalog Governance

All tables follow the three-level Unity Catalog namespace:

```
<catalog>.<schema>.<table>

prod_catalog.raw.bp_raw_kafka
prod_catalog.trusted.bp_trusted
prod_catalog.gold.bp_gold_unified
```

Access control is enforced at the catalog level. Pipelines run under service principals with scoped grants. No direct user access to RAW or TRUSTED tables in production — only Gold is exposed to analysts.

---

## Checkpoint Strategy

SDP manages checkpoints automatically per flow. For the Kafka Sink (direct `writeStream`), checkpoints are explicitly set:

```python
.option("checkpointLocation", "/checkpoints/bp_sink_kafka")
```

Full pipeline refresh (clearing all checkpoints + table data) is required when:
- Delta table schema changes incompatibly
- Unity Catalog table ID changes (e.g., after `DROP TABLE`)
- The `target` schema is remapped to a new catalog path
