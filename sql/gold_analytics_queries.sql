-- Curated analytics queries for Snowflake or Databricks SQL.

-- 1. Daily symbol-level notional.
SELECT
    trade_date,
    symbol,
    currency,
    SUM(trade_count) AS trade_count,
    SUM(gross_notional) AS gross_notional,
    SUM(net_signed_notional) AS net_signed_notional
FROM curated.daily_trade_summary
GROUP BY trade_date, symbol, currency
ORDER BY trade_date, symbol;

-- 2. Accounts in review with material exposure.
SELECT
    account_id,
    customer_id,
    region,
    status,
    trade_count,
    gross_notional,
    net_signed_notional
FROM curated.account_position_summary
WHERE status IN ('REVIEW', 'SUSPENDED')
   OR gross_notional >= 100000
ORDER BY gross_notional DESC;

-- 3. Reconciliation failures and warnings.
SELECT
    audit_ts,
    check_name,
    source_table,
    target_table,
    status,
    metric_value,
    details
FROM curated.reconciliation_results
WHERE status <> 'PASS'
ORDER BY audit_ts DESC;

-- 4. Buy/sell imbalance by symbol.
SELECT
    symbol,
    SUM(CASE WHEN side = 'BUY' THEN gross_notional ELSE 0 END) AS buy_notional,
    SUM(CASE WHEN side = 'SELL' THEN gross_notional ELSE 0 END) AS sell_notional,
    SUM(net_signed_notional) AS net_signed_notional
FROM curated.daily_trade_summary
GROUP BY symbol
ORDER BY ABS(SUM(net_signed_notional)) DESC;
