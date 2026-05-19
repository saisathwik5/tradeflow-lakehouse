"""Reconciliation checks emitted as audit-friendly Spark DataFrames."""

from __future__ import annotations

from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _result(
    spark: SparkSession,
    check_name: str,
    source: str,
    target: str,
    status: str,
    metric_value: str,
    details: str,
) -> DataFrame:
    return spark.createDataFrame(
        [(check_name, source, target, status, metric_value, details)],
        "check_name string, source_table string, target_table string, "
        "status string, metric_value string, details string",
    ).withColumn("audit_ts", F.current_timestamp())


def row_count_match(
    source_df: DataFrame, target_df: DataFrame, source: str, target: str
) -> DataFrame:
    source_count = source_df.count()
    target_count = target_df.count()
    status = "PASS" if source_count == target_count else "FAIL"
    return _result(
        source_df.sparkSession,
        "row_count_match",
        source,
        target,
        status,
        f"{source_count}:{target_count}",
        f"source_count={source_count}, target_count={target_count}",
    )


def primary_key_duplicates(df: DataFrame, table: str, key_columns: Iterable[str]) -> DataFrame:
    duplicate_count = df.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
    status = "PASS" if duplicate_count == 0 else "FAIL"
    return _result(
        df.sparkSession,
        "primary_key_duplicates",
        table,
        table,
        status,
        str(duplicate_count),
        f"duplicate_key_count={duplicate_count}",
    )


def checksum_match(
    source_df: DataFrame,
    target_df: DataFrame,
    source: str,
    target: str,
    columns: Iterable[str],
) -> DataFrame:
    checksum_expr = F.sum(F.xxhash64(*[F.col(c).cast("string") for c in columns]))
    source_checksum = source_df.select(checksum_expr.alias("checksum")).first()["checksum"]
    target_checksum = target_df.select(checksum_expr.alias("checksum")).first()["checksum"]
    status = "PASS" if source_checksum == target_checksum else "FAIL"
    return _result(
        source_df.sparkSession,
        "checksum_match",
        source,
        target,
        status,
        f"{source_checksum}:{target_checksum}",
        f"columns={','.join(columns)}",
    )


def null_threshold(df: DataFrame, table: str, column_name: str, max_null_ratio: float) -> DataFrame:
    total_count = df.count()
    null_count = df.filter(F.col(column_name).isNull()).count()
    null_ratio = null_count / total_count if total_count else 0.0
    status = "PASS" if null_ratio <= max_null_ratio else "FAIL"
    return _result(
        df.sparkSession,
        "null_threshold",
        table,
        table,
        status,
        f"{null_ratio:.6f}",
        f"column={column_name}, nulls={null_count}, rows={total_count}",
    )


def balance_validation(trades_df: DataFrame, table: str) -> DataFrame:
    imbalance = trades_df.agg(F.round(F.sum("signed_notional"), 2).alias("net")).first()["net"]
    status = "PASS" if imbalance == 0 else "WARN"
    return _result(
        trades_df.sparkSession,
        "balance_validation",
        table,
        table,
        status,
        str(imbalance),
        "Net signed notional should be explained by inventory movement.",
    )


def union_results(results: list[DataFrame]) -> DataFrame:
    """Union reconciliation result frames with a shared schema."""
    if not results:
        raise ValueError("At least one reconciliation result is required.")
    combined = results[0]
    for result in results[1:]:
        combined = combined.unionByName(result)
    return combined
