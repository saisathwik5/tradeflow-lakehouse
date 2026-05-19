# Resume Notes

## Short Description

TradeFlow Lakehouse is a Spark-based financial transaction lakehouse that
implements raw, refined, and curated data layers with SCD Type 2 temporal
modeling, schema evolution handling, reconciliation checks, and Snowflake-ready
analytics exports.

## Best Bullets

- Built a medallion lakehouse pipeline with Apache Spark to process financial
  trade and account events through bronze, silver, and gold layers.
- Implemented SCD Type 2 account state modeling with `valid_from`, `valid_to`,
  and `current_flag` columns to reconstruct historical customer/account state.
- Developed reconciliation checks for row counts, duplicate keys, checksums,
  null thresholds, and signed-notional balance validation.
- Simulated schema evolution across JSON event batches and implemented backward
  compatible transformations for nullable evolved fields.
- Added Iceberg time-travel SQL, Snowflake DDL, Databricks job configuration,
  pytest tests, and GitHub Actions CI.

## Interview Talking Points

- Why SCD2 is better than overwriting account status for audit use cases.
- How checksum reconciliation catches silent value drift missed by row counts.
- How Iceberg snapshots and time travel support incident investigation.
- Why schema evolution needs defaults and compatibility tests.
- How raw, refined, and curated layers separate auditability from analytics.
