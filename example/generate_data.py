# Databricks notebook source

# MAGIC %md
# MAGIC # Generate IoT telemetry
# MAGIC
# MAGIC Generate fake IoT telemetry with dbldatagen and land it as parquet.
# MAGIC
# MAGIC Writes one batch of parquet files per device fleet into the UC volume. Run it
# MAGIC repeatedly (it appends new files with fresh timestamps) so the streaming
# MAGIC pipeline has continuously arriving data to process.
# MAGIC
# MAGIC We intentionally inject a small fraction of null / out-of-range readings so the
# MAGIC data-quality metrics in the custom observation have something to report on.


# COMMAND ----------
# MAGIC %pip install dbldatagen pyyaml

# COMMAND ----------

from __future__ import annotations

import os
import time

import dbldatagen as dg
import yaml
from pyspark.sql import SparkSession

# COMMAND ----------

# Load shared config from the single YAML file.
with open(os.path.join(os.getcwd(), "example_config.yaml")) as f:
    CFG = yaml.safe_load(f)

FLEETS = CFG["fleets"]
VOLUME_PATH = CFG["volume_path"]


def landing_path(fleet: str) -> str:
    return f"{VOLUME_PATH}/{fleet}"


ROWS_PER_BATCH = 50_000

# COMMAND ----------


def _spec(spark: SparkSession, fleet: str) -> dg.DataGenerator:
    """Build the dbldatagen spec for a fleet of IoT sensors."""
    num_devices = 200
    return (
        dg.DataGenerator(spark, name=f"iot_{fleet}", rows=ROWS_PER_BATCH, partitions=4)
        .withColumn("device_id", "string", template=rf"{fleet}-dev-\d\d\d")
        .withColumn(
            "reading_ts",
            "timestamp",
            begin="2026-01-01 00:00:00",
            end="2026-12-31 23:59:59",
            random=True,
        )
        # ~2% nulls and a wide range so some readings are out-of-range.
        .withColumn(
            "temperature_c",
            "double",
            minValue=-40.0,
            maxValue=130.0,
            random=True,
            percentNulls=0.02,
        )
        .withColumn(
            "humidity_pct",
            "double",
            minValue=0.0,
            maxValue=100.0,
            random=True,
            percentNulls=0.01,
        )
        .withColumn("battery_pct", "double", minValue=0.0, maxValue=100.0, random=True)
        .withColumn("device_count_hint", "int", minValue=num_devices, maxValue=num_devices)
    )


def generate_batch(spark: SparkSession) -> None:
    for fleet in FLEETS:
        df = _spec(spark, fleet).build()
        (df.repartition(50).write.mode("append").parquet(landing_path(fleet)))
        print(f"Wrote {ROWS_PER_BATCH} rows for {fleet} -> {landing_path(fleet)}")

# COMMAND ----------

# Always on loop to simulate data always arriving
while True: 
    generate_batch(spark)
    time.sleep(60)
