from pyspark.sql import functions as F

from tradeflow.gold import daily_trade_summary
from tradeflow.silver import refine_trades


def test_schema_evolution_defaults_old_trade_events(spark):
    bronze = spark.createDataFrame(
        [
            ("T1", "A1", "GS", "BUY", 10, 100.0, "2026-01-01T10:00:00Z"),
        ],
        "trade_id string, account_id string, symbol string, side string, "
        "quantity long, price double, trade_ts string",
    )

    refined = refine_trades(bronze)
    row = refined.first()

    assert "currency" in refined.columns
    assert "venue" in refined.columns
    assert row["currency"] == "USD"
    assert row["venue"] == "UNKNOWN"


def test_trade_validation_and_notional(spark):
    bronze = spark.createDataFrame(
        [
            ("T1", "A1", "GS", "BUY", 10, 100.0, "2026-01-01T10:00:00Z", "USD"),
            ("T2", "A1", "GS", "SELL", 5, 110.0, "2026-01-01T11:00:00Z", "USD"),
            ("T3", "A1", "GS", "BUY", -1, 99.0, "2026-01-01T12:00:00Z", "USD"),
        ],
        "trade_id string, account_id string, symbol string, side string, "
        "quantity long, price double, trade_ts string, currency string",
    )

    refined = refine_trades(bronze)
    valid = refined.filter(F.col("validation_status") == "VALID").count()
    invalid = refined.filter(F.col("validation_status") != "VALID").count()
    signed = refined.filter(F.col("trade_id") == "T2").first()["signed_notional"]

    assert valid == 2
    assert invalid == 1
    assert signed == -550.0


def test_gold_daily_summary_uses_only_valid_trades(spark):
    bronze = spark.createDataFrame(
        [
            ("T1", "A1", "GS", "BUY", 10, 100.0, "2026-01-01T10:00:00Z", "USD"),
            ("T2", "A1", "GS", "BUY", -2, 101.0, "2026-01-01T11:00:00Z", "USD"),
        ],
        "trade_id string, account_id string, symbol string, side string, "
        "quantity long, price double, trade_ts string, currency string",
    )

    summary = daily_trade_summary(refine_trades(bronze))
    row = summary.first()

    assert summary.count() == 1
    assert row["trade_count"] == 1
    assert row["gross_notional"] == 1000.0
