"""Silver layer transformations for validated, typed trade records."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

EVOLVED_TRADE_COLUMNS = {
    "currency": "USD",
    "venue": "UNKNOWN",
    "settlement_date": None,
}


def normalize_trade_schema(df: DataFrame) -> DataFrame:
    """Backfill nullable evolved columns so old event batches still load."""
    normalized = df
    for column_name, default_value in EVOLVED_TRADE_COLUMNS.items():
        if column_name not in normalized.columns:
            normalized = normalized.withColumn(column_name, F.lit(default_value))
    return normalized


def refine_trades(bronze_trades: DataFrame) -> DataFrame:
    """Validate, deduplicate, and type raw trade events."""
    df = normalize_trade_schema(bronze_trades)
    if "ingestion_ts" not in df.columns:
        df = df.withColumn("ingestion_ts", F.to_timestamp(F.lit("1970-01-01T00:00:00Z")))

    typed = (
        df.withColumn("trade_ts", F.to_timestamp("trade_ts"))
        .withColumn("settlement_date", F.to_date("settlement_date"))
        .withColumn("quantity", F.col("quantity").cast("long"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("notional", F.round(F.col("quantity") * F.col("price"), 2))
        .withColumn(
            "signed_notional",
            F.when(F.col("side") == "BUY", F.col("notional")).otherwise(-F.col("notional")),
        )
        .withColumn(
            "validation_status",
            F.when(F.col("trade_id").isNull(), "INVALID_MISSING_TRADE_ID")
            .when(F.col("quantity") <= 0, "INVALID_QUANTITY")
            .when(F.col("price") <= 0, "INVALID_PRICE")
            .when(~F.col("side").isin("BUY", "SELL"), "INVALID_SIDE")
            .otherwise("VALID"),
        )
    )

    dedupe_order = F.row_number().over(
        Window.partitionBy("trade_id").orderBy(F.col("ingestion_ts").desc())
    )
    return (
        typed.withColumn("dedupe_rank", dedupe_order)
        .filter(F.col("dedupe_rank") == 1)
        .drop("dedupe_rank")
    )


def refine_account_events(bronze_accounts: DataFrame) -> DataFrame:
    """Type account change events before SCD2 processing."""
    return (
        bronze_accounts.withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("credit_limit", F.col("credit_limit").cast("double"))
        .withColumn("risk_score", F.col("risk_score").cast("integer"))
    )
