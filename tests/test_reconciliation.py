from tradeflow.reconciliation import (
    checksum_match,
    null_threshold,
    primary_key_duplicates,
    row_count_match,
)


def test_row_count_match_passes_for_equal_counts(spark):
    left = spark.createDataFrame([(1,), (2,)], "id integer")
    right = spark.createDataFrame([(3,), (4,)], "id integer")

    result = row_count_match(left, right, "left", "right").first()

    assert result["status"] == "PASS"


def test_duplicate_detection_fails_on_replayed_key(spark):
    df = spark.createDataFrame([("T1",), ("T1",), ("T2",)], "trade_id string")

    result = primary_key_duplicates(df, "silver.trades", ["trade_id"]).first()

    assert result["status"] == "FAIL"
    assert result["metric_value"] == "1"


def test_null_threshold_flags_column_degradation(spark):
    df = spark.createDataFrame([(None,), ("USD",)], "currency string")

    result = null_threshold(df, "silver.trades", "currency", 0.0).first()

    assert result["status"] == "FAIL"


def test_checksum_match_passes_for_same_values(spark):
    df = spark.createDataFrame([("T1", "GS"), ("T2", "MS")], "trade_id string, symbol string")

    result = checksum_match(df, df, "a", "b", ["trade_id", "symbol"]).first()

    assert result["status"] == "PASS"
