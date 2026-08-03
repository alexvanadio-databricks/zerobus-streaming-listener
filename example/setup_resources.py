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
# MAGIC %pip install pyyaml

# COMMAND ----------

import os
import sys

import yaml

sys.path.insert(0, os.path.join(os.getcwd(), "..", "src"))

from pyspark.sql import SparkSession

from streaming_listener.schema import create_table_ddl

# COMMAND ----------

# Load shared config from the single YAML file.
with open(os.path.join(os.getcwd(), "example_config.yaml")) as f:
    CFG = yaml.safe_load(f)

CATALOG = CFG["catalog"]
SCHEMA = CFG["schema"]
VOLUME = CFG["volume"]
METRICS_TABLE = CFG["metrics_table"]

# COMMAND ----------


spark = SparkSession.builder.getOrCreate()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(create_table_ddl(METRICS_TABLE))

print(f"Ensured schema {CATALOG}.{SCHEMA}, volume {VOLUME}, table {METRICS_TABLE}")

