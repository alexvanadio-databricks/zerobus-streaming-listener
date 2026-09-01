# Databricks notebook source

# MAGIC %md
# MAGIC # Setup resources
# MAGIC
# MAGIC Create the catalog schema, landing volume, and Zerobus metrics table.
# MAGIC
# MAGIC Run once before the pipeline. Idempotent: uses IF NOT EXISTS throughout.
# MAGIC The metrics-table DDL comes from the core package so the schema stays in one
# MAGIC place.

# COMMAND ----------

from pyspark.sql import SparkSession

# streaming_listener is installed on the cluster as a wheel library by the
# bundle, so it imports directly (no sys.path hacking).
from streaming_listener.schema import create_table_ddl

# COMMAND ----------

# Config arrives as job parameters (widgets). The defaults let the notebook run
# interactively outside the bundle too.
dbutils.widgets.text("catalog", "classic_stable_qkee68_catalog")  # noqa: F821
dbutils.widgets.text("schema", "streaming_monitor")  # noqa: F821
dbutils.widgets.text("volume", "raw_landing")  # noqa: F821

CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
VOLUME = dbutils.widgets.get("volume")  # noqa: F821
METRICS_TABLE = f"{CATALOG}.{SCHEMA}.query_metrics"

# COMMAND ----------


spark = SparkSession.builder.getOrCreate()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(create_table_ddl(METRICS_TABLE))

print(f"Ensured schema {CATALOG}.{SCHEMA}, volume {VOLUME}, table {METRICS_TABLE}")
