-- Iceberg examples for Databricks or Spark SQL with an Iceberg catalog.
-- Replace local.tradeflow with your configured catalog and namespace.

CREATE NAMESPACE IF NOT EXISTS local.tradeflow;

CREATE TABLE IF NOT EXISTS local.tradeflow.silver_trades (
    trade_id STRING,
    account_id STRING,
    symbol STRING,
    side STRING,
    quantity BIGINT,
    price DOUBLE,
    trade_ts TIMESTAMP,
    currency STRING,
    venue STRING,
    settlement_date DATE,
    notional DOUBLE,
    signed_notional DOUBLE,
    validation_status STRING,
    ingestion_ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(trade_ts), symbol);

-- Schema evolution: add nullable producer fields without rewriting history.
ALTER TABLE local.tradeflow.silver_trades
ADD COLUMN IF NOT EXISTS execution_algo STRING;

-- Partition evolution: future writes can use month-level partitions.
ALTER TABLE local.tradeflow.silver_trades
ADD PARTITION FIELD months(trade_ts);

-- Snapshot inspection for auditability.
SELECT
    committed_at,
    snapshot_id,
    parent_id,
    operation
FROM local.tradeflow.silver_trades.snapshots
ORDER BY committed_at DESC;

-- Time travel by snapshot id.
SELECT *
FROM local.tradeflow.silver_trades VERSION AS OF 1234567890123456789
WHERE account_id = 'A100';

-- Time travel by timestamp.
SELECT *
FROM local.tradeflow.silver_trades TIMESTAMP AS OF '2026-02-01 00:00:00'
WHERE symbol = 'GS';
