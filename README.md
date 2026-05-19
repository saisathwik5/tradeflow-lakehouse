# TradeFlow Lakehouse

Production-style financial transaction lakehouse focused on temporal correctness,
schema evolution, reconciliation, and operational data quality.

This project is intentionally not a dashboard-first ETL demo. It models the parts
of enterprise data engineering that matter in regulated environments: raw to
refined to curated layers, historical state, deterministic reconciliation, schema
drift handling, and warehouse-ready analytics tables.

## Architecture

```text
JSON trade and account events
        |
        v
Bronze raw tables
  - append-only payloads
  - ingestion metadata
  - schema drift preserved
        |
        v
Silver refined tables
  - typed records
  - deduplication
  - validation flags
  - SCD Type 2 account state
        |
        v
Gold curated tables
  - daily trade facts
  - account exposure
  - reconciliation metrics
        |
        v
Snowflake analytics
```

## What This Demonstrates

- **Production-ready pipelines:** idempotent local runner, deterministic sample
  data, CI, tests, typed transformations, and audit columns.
- **Temporal data modeling:** SCD Type 2 account dimension with `valid_from`,
  `valid_to`, and `current_flag`.
- **Data reconciliation:** row-count, duplicate, checksum, null threshold, and
  accounting balance checks emitted as structured audit records.
- **Iceberg / Databricks / Snowflake:** local Spark-compatible implementation
  plus Iceberg catalog config, Databricks job template, Snowflake DDL, and export
  SQL.
- **Schema evolution:** sample events add fields over time, and transformations
  support nullable evolved columns.
- **Spark + SQL:** PySpark transformations and deployment SQL for curated tables.
- **Testing + monitoring:** pytest coverage for temporal logic,
  reconciliation, schema evolution, and layer transforms.
- **Raw -> refined -> curated:** bronze, silver, and gold modules map directly to
  lakehouse layers.

## Project Structure

```text
financial-lakehouse/
├── tradeflow/                 # PySpark pipeline package
│   ├── bronze.py              # Raw ingestion
│   ├── silver.py              # Validation and deduplication
│   ├── temporal.py            # SCD Type 2 logic
│   ├── reconciliation.py      # Audit checks and metrics
│   ├── gold.py                # Curated analytics tables
│   ├── pipeline.py            # End-to-end local runner
│   └── spark.py               # Spark/Iceberg session helpers
├── sample_data/               # Deterministic schema-drift fixtures
├── sql/                       # Iceberg, Snowflake, and analytics SQL
├── databricks/                # Job/notebook deployment assets
├── docs/                      # Architecture and resume notes
├── tests/                     # Unit tests for engineering logic
└── .github/workflows/ci.yml   # Lint and test workflow
```

## Quickstart

```bash
cd financial-lakehouse
pip install -r requirements.txt
python -m tradeflow.pipeline --input sample_data --warehouse lakehouse
pytest tests -v
```

The local runner writes Parquet tables under `lakehouse/` so the project can be
reviewed without cloud credentials. In Databricks, use the Iceberg session
settings in `tradeflow/spark.py` and the job template in `databricks/job.yml`.

## Example Outputs

After a pipeline run:

```text
lakehouse/
├── bronze/trades
├── bronze/account_events
├── silver/trades
├── silver/account_scd2
├── gold/daily_trade_summary
├── gold/account_position_summary
└── audit/reconciliation_results
```

## Reconciliation Checks

| Check | Purpose |
| --- | --- |
| `row_count_match` | Detect dropped or inflated records across layers |
| `primary_key_duplicates` | Catch replay and producer retry issues |
| `checksum_match` | Validate value-level consistency after transformation |
| `null_threshold` | Monitor required-column degradation |
| `balance_validation` | Confirm buys and sells reconcile to signed notional |

## Schema Evolution Demo

`sample_data/trades/day_01.json` starts with core trade fields. Later files add
nullable fields such as `currency`, `venue`, and `settlement_date`. The silver
transform normalizes missing evolved columns and preserves backward
compatibility.

For Iceberg time travel and partition evolution examples, see
[`sql/iceberg_time_travel.sql`](sql/iceberg_time_travel.sql).

## Resume Bullets

- Built a medallion lakehouse pipeline using Apache Spark to process financial
  trade events across bronze, silver, and gold layers with audit metadata.
- Implemented SCD Type 2 temporal account modeling to reconstruct historical
  account state with `valid_from`, `valid_to`, and `current_flag` semantics.
- Developed a reconciliation framework for row counts, duplicate detection,
  checksums, null thresholds, and signed-notional balance validation.
- Simulated schema evolution across JSON event batches and implemented backward
  compatible Spark transformations for evolved trade columns.
- Added Snowflake DDL, Iceberg time-travel SQL, Databricks job configuration,
  pytest coverage, and CI checks to signal production engineering discipline.

## Cloud Deployment Notes

- **Databricks:** import `databricks/notebooks/tradeflow_job.py` and use
  `databricks/job.yml` as the job template.
- **Iceberg:** configure a Hadoop or REST catalog using the helper in
  `tradeflow/spark.py`; then write tables with `USING iceberg`.
- **Snowflake:** create analytics tables using `sql/snowflake_ddl.sql`, then
  load curated Parquet exports from object storage.
