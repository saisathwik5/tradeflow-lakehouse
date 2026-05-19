"""Temporal data modeling utilities."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_account_scd2(account_events: DataFrame) -> DataFrame:
    """Build a Type 2 dimension for account state changes."""
    entity_window = Window.partitionBy("account_id").orderBy("event_ts")
    change_columns = ["status", "region", "credit_limit", "risk_score"]

    hash_inputs = [
        F.coalesce(F.col(column_name).cast("string"), F.lit("")) for column_name in change_columns
    ]
    with_hashes = account_events.withColumn(
        "state_hash",
        F.sha2(F.concat_ws("||", *hash_inputs), 256),
    ).withColumn("previous_hash", F.lag("state_hash").over(entity_window))

    changed = with_hashes.filter(
        F.col("previous_hash").isNull() | (F.col("state_hash") != F.col("previous_hash"))
    )

    return (
        changed.withColumn("valid_from", F.col("event_ts"))
        .withColumn("valid_to", F.lead("event_ts").over(entity_window))
        .withColumn("current_flag", F.col("valid_to").isNull())
        .select(
            "account_id",
            "customer_id",
            "status",
            "region",
            "credit_limit",
            "risk_score",
            "valid_from",
            "valid_to",
            "current_flag",
            "state_hash",
        )
    )
