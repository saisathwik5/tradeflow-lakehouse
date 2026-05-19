from pyspark.sql import functions as F

from tradeflow.silver import refine_account_events
from tradeflow.temporal import build_account_scd2


def test_account_scd2_closes_previous_state(spark):
    events = spark.createDataFrame(
        [
            ("A1", "C1", "ACTIVE", "AMER", 100000.0, 3, "2026-01-01T00:00:00Z"),
            ("A1", "C1", "ACTIVE", "AMER", 150000.0, 2, "2026-02-01T00:00:00Z"),
            ("A1", "C1", "ACTIVE", "AMER", 150000.0, 2, "2026-02-02T00:00:00Z"),
        ],
        "account_id string, customer_id string, status string, region string, "
        "credit_limit double, risk_score integer, event_ts string",
    )

    scd2 = build_account_scd2(refine_account_events(events))
    rows = scd2.orderBy("valid_from").collect()

    assert len(rows) == 2
    assert rows[0]["current_flag"] is False
    assert rows[0]["valid_to"] == rows[1]["valid_from"]
    assert rows[1]["current_flag"] is True


def test_account_scd2_can_reconstruct_historical_state(spark):
    events = spark.createDataFrame(
        [
            ("A1", "C1", "ACTIVE", "AMER", 100000.0, 3, "2026-01-01T00:00:00Z"),
            ("A1", "C1", "REVIEW", "AMER", 100000.0, 8, "2026-03-01T00:00:00Z"),
        ],
        "account_id string, customer_id string, status string, region string, "
        "credit_limit double, risk_score integer, event_ts string",
    )

    scd2 = build_account_scd2(refine_account_events(events))
    point_in_time = scd2.filter(
        (F.col("valid_from") <= F.to_timestamp(F.lit("2026-02-15T00:00:00Z")))
        & (
            F.col("valid_to").isNull()
            | (F.col("valid_to") > F.to_timestamp(F.lit("2026-02-15T00:00:00Z")))
        )
    ).first()

    assert point_in_time["status"] == "ACTIVE"
    assert point_in_time["risk_score"] == 3
