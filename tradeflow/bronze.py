"""Bronze layer ingestion for raw append-only event files."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_raw_json(spark: SparkSession, path: str, source_name: str) -> DataFrame:
    """Read raw JSON while attaching ingestion metadata."""
    return (
        spark.read.option("multiLine", "false")
        .json(path)
        .withColumn("source_file", F.input_file_name())
        .withColumn("source_name", F.lit(source_name))
        .withColumn("ingestion_ts", F.current_timestamp())
    )


def write_bronze(df: DataFrame, output_path: str) -> None:
    """Persist a bronze table in appendable Parquet form for local runs."""
    df.write.mode("overwrite").parquet(output_path)
