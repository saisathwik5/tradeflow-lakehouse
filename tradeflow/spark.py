"""Spark session helpers for local and Iceberg-backed runs."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def _configure_python_worker() -> str:
    """Use the current interpreter for PySpark driver and worker processes."""
    python_executable = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_executable)
    return python_executable


def create_spark(app_name: str = "tradeflow-lakehouse") -> SparkSession:
    """Create a local Spark session used by tests and the sample runner."""
    python_executable = _configure_python_worker()
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
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
    python_executable = _configure_python_worker()
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
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .getOrCreate()
    )
