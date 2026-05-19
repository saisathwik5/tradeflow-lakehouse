"""Spark session helpers for local and Iceberg-backed runs."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession


def create_spark(app_name: str = "tradeflow-lakehouse") -> SparkSession:
    """Create a local Spark session used by tests and the sample runner."""
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def create_iceberg_spark(
    warehouse: str | Path,
    app_name: str = "tradeflow-iceberg",
    catalog: str = "local",
) -> SparkSession:
    """Create a Spark session configured for an Iceberg Hadoop catalog.

    The Iceberg runtime jar is supplied at job-submit time in Databricks or with
    PYSPARK_SUBMIT_ARGS locally. Keeping the config here makes the project easy
    to move between a laptop, Databricks, and CI.
    """
    warehouse_path = str(Path(warehouse).resolve())
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse_path)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
