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

from __future__ import annotations

import time

import dbldatagen as dg
from pyspark.sql import SparkSession

# COMMAND ----------

# Config arrives as job parameters (widgets); defaults allow interactive runs.
dbutils.widgets.text("catalog", "classic_stable_qkee68_catalog")  # noqa: F821
dbutils.widgets.text("schema", "streaming_monitor")  # noqa: F821
dbutils.widgets.text("volume", "raw_landing")  # noqa: F821
dbutils.widgets.text("fleets", "fleet_a,fleet_b")  # noqa: F821
dbutils.widgets.text("num_batches", "3")  # noqa: F821

CATALOG = dbutils.widgets.get("catalog")  # noqa: F821
SCHEMA = dbutils.widgets.get("schema")  # noqa: F821
VOLUME = dbutils.widgets.get("volume")  # noqa: F821
FLEETS = [f.strip() for f in dbutils.widgets.get("fleets").split(",") if f.strip()]  # noqa: F821
NUM_BATCHES = int(dbutils.widgets.get("num_batches"))  # noqa: F821
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"


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

# Bounded run: land NUM_BATCHES batches so this task finishes and the pipeline
# has data to drain. Raise num_batches (or restore a while-True loop) to
# simulate continuously arriving telemetry.
for _batch in range(NUM_BATCHES):  # noqa: F821
    generate_batch(spark)  # noqa: F821
    if _batch < NUM_BATCHES - 1:
        time.sleep(5)
