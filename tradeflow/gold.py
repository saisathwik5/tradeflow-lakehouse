"""Gold layer curated analytics tables."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def daily_trade_summary(trades_df: DataFrame) -> DataFrame:
    """Aggregate validated trades by trade date, symbol, side, and currency."""
    return (
        trades_df.filter(F.col("validation_status") == "VALID")
        .withColumn("trade_date", F.to_date("trade_ts"))
        .groupBy("trade_date", "symbol", "side", "currency")
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("quantity").alias("total_quantity"),
            F.round(F.sum("notional"), 2).alias("gross_notional"),
            F.round(F.sum("signed_notional"), 2).alias("net_signed_notional"),
            F.round(F.avg("price"), 4).alias("avg_price"),
        )
    )


def account_position_summary(trades_df: DataFrame, account_scd2: DataFrame) -> DataFrame:
    """Join trades to the account state valid at the trade timestamp."""
    trades = trades_df.alias("trades")
    accounts = account_scd2.alias("accounts")
    current_state = trades.join(
        accounts,
        (F.col("trades.account_id") == F.col("accounts.account_id"))
        & (F.col("trades.trade_ts") >= F.col("accounts.valid_from"))
        & (
            F.col("accounts.valid_to").isNull()
            | (F.col("trades.trade_ts") < F.col("accounts.valid_to"))
        ),
        "left",
    )

    return (
        current_state.filter(F.col("validation_status") == "VALID")
        .groupBy(
            F.col("trades.account_id").alias("account_id"),
            "customer_id",
            "region",
            "status",
        )
        .agg(
            F.count("*").alias("trade_count"),
            F.round(F.sum("signed_notional"), 2).alias("net_signed_notional"),
            F.round(F.sum(F.abs("signed_notional")), 2).alias("gross_notional"),
        )
    )
