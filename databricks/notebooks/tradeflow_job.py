# Databricks notebook source
# MAGIC %md
# MAGIC # TradeFlow Lakehouse Job
# MAGIC Runs the bronze, silver, gold, and reconciliation pipeline.

# COMMAND ----------

dbutils.widgets.text("input_path", "dbfs:/tradeflow/sample_data")
dbutils.widgets.text("warehouse_path", "dbfs:/tradeflow/lakehouse")

input_path = dbutils.widgets.get("input_path")
warehouse_path = dbutils.widgets.get("warehouse_path")

# COMMAND ----------

from tradeflow.pipeline import run_pipeline

run_pipeline(input_path, warehouse_path)
