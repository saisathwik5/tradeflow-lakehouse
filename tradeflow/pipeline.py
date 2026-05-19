"""End-to-end local runner for the TradeFlow medallion pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from tradeflow.bronze import read_raw_json, write_bronze
from tradeflow.gold import account_position_summary, daily_trade_summary
from tradeflow.reconciliation import (
    balance_validation,
    checksum_match,
    null_threshold,
    primary_key_duplicates,
    row_count_match,
    union_results,
)
from tradeflow.silver import refine_account_events, refine_trades
from tradeflow.spark import create_spark
from tradeflow.temporal import build_account_scd2


def run_pipeline(input_dir: str | Path, warehouse_dir: str | Path) -> None:
    """Run bronze, silver, gold, and audit layers locally."""
    input_path = Path(input_dir)
    warehouse_path = Path(warehouse_dir)
    spark = create_spark()

    bronze_trades = read_raw_json(spark, str(input_path / "trades" / "*.json"), "trade_events")
    bronze_accounts = read_raw_json(
        spark,
        str(input_path / "account_events" / "*.json"),
        "account_events",
    )

    silver_trades = refine_trades(bronze_trades)
    silver_accounts = refine_account_events(bronze_accounts)
    account_scd2 = build_account_scd2(silver_accounts)
    gold_daily = daily_trade_summary(silver_trades)
    gold_accounts = account_position_summary(silver_trades, account_scd2)

    results = union_results(
        [
            row_count_match(bronze_trades, silver_trades, "bronze.trades", "silver.trades"),
            primary_key_duplicates(silver_trades, "silver.trades", ["trade_id"]),
            checksum_match(
                silver_trades,
                silver_trades,
                "silver.trades",
                "silver.trades",
                ["trade_id", "account_id", "symbol", "side", "quantity", "price"],
            ),
            null_threshold(silver_trades, "silver.trades", "currency", 0.0),
            balance_validation(silver_trades, "silver.trades"),
        ]
    )

    write_bronze(bronze_trades, str(warehouse_path / "bronze" / "trades"))
    write_bronze(bronze_accounts, str(warehouse_path / "bronze" / "account_events"))
    silver_trades.write.mode("overwrite").parquet(str(warehouse_path / "silver" / "trades"))
    account_scd2.write.mode("overwrite").parquet(str(warehouse_path / "silver" / "account_scd2"))
    gold_daily.write.mode("overwrite").parquet(str(warehouse_path / "gold" / "daily_trade_summary"))
    gold_accounts.write.mode("overwrite").parquet(
        str(warehouse_path / "gold" / "account_position_summary")
    )
    results.write.mode("overwrite").parquet(
        str(warehouse_path / "audit" / "reconciliation_results")
    )

    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the TradeFlow Lakehouse pipeline.")
    parser.add_argument("--input", default="sample_data", help="Input sample data directory.")
    parser.add_argument("--warehouse", default="lakehouse", help="Output warehouse directory.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_pipeline(args.input, args.warehouse)
