# Databricks notebook source

# MAGIC %md
# MAGIC # IoT streaming pipeline
# MAGIC
# MAGIC IoT streaming example that exercises the reusable Zerobus listener pattern.
# MAGIC
# MAGIC Flow, per device fleet:
# MAGIC
# MAGIC ```
# MAGIC parquet in UC volume --readStream--> bronze delta table
# MAGIC bronze --readStream--> windowed aggregation delta table (with df.observe)
# MAGIC ```
# MAGIC
# MAGIC Two fleets run as two independent streaming queries in the same Spark session,
# MAGIC so both log their metrics to the *same* Zerobus metrics table through the single
# MAGIC registered `ZerobusStreamingQueryListener`.

# COMMAND ----------
# MAGIC %pip install databricks-zerobus-ingest-sdk>=0.1.0 pyyaml

# COMMAND ----------

from __future__ import annotations

import os
import sys

import yaml

# Make the core package (streaming_listener) importable regardless of how the
# job launches this file.
sys.path.insert(0, os.path.join(os.getcwd(), "..", "src"))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# The core reusable pattern:
from streaming_listener import ZerobusConfig, ZerobusStreamingQueryListener

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load configuration
# MAGIC
# MAGIC Everything is read from `example_config.yaml`. The Zerobus client secret is
# MAGIC never stored in the file — it is pulled at runtime from the secret scope named
# MAGIC in the YAML.

# COMMAND ----------

with open(os.path.join(os.getcwd(), "example_config.yaml")) as f:
    CFG = yaml.safe_load(f)

CATALOG = CFG["catalog"]
SCHEMA = CFG["schema"]
VOLUME_PATH = CFG["volume_path"]
FLEETS = CFG["fleets"]
METRICS_TABLE = CFG["metrics_table"]

ZB = CFG["zerobus"]
CLIENT_ID = ZB["client_id"]
# The secret lives in a Databricks secret scope, not in the YAML.
CLIENT_SECRET = dbutils.secrets.get(ZB["secret_scope"], ZB["client_secret_key"])  # noqa: F821


def landing_path(fleet: str) -> str:
    return f"{VOLUME_PATH}/{fleet}"


def bronze_table(fleet: str) -> str:
    return f"{CATALOG}.{SCHEMA}.bronze_{fleet}"


def agg_table(fleet: str) -> str:
    return f"{CATALOG}.{SCHEMA}.agg_{fleet}"


def checkpoint_path(name: str) -> str:
    return f"{VOLUME_PATH}/_checkpoints/{name}"

# COMMAND ----------

# Schema of the parquet the generator lands (explicit so readStream is safe).
RAW_SCHEMA = StructType(
    [
        StructField("device_id", StringType()),
        StructField("reading_ts", TimestampType()),
        StructField("temperature_c", DoubleType()),
        StructField("humidity_pct", DoubleType()),
        StructField("battery_pct", DoubleType()),
        StructField("device_count_hint", IntegerType()),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The observation
# MAGIC
# MAGIC `df.observe(name, *metrics)` attaches metrics that Spark computes over each
# MAGIC micro-batch and reports through the `StreamingQueryListener` progress event
# MAGIC (as `observedMetrics`). Our listener serializes those into the
# MAGIC `observed_metrics` variant column. Below it is applied directly to the bronze
# MAGIC stream so it is obvious *when* and *how* the observation runs.

# COMMAND ----------

# Physically plausible sensor ranges; readings outside are "bad".
TEMP_MIN_C, TEMP_MAX_C = -40.0, 85.0
HUMIDITY_MIN, HUMIDITY_MAX = 0.0, 100.0


def with_iot_observation(df: DataFrame, name: str) -> DataFrame:
    """Attach the IoT data-quality + value observation to a streaming DataFrame."""
    temp = F.col("temperature_c")
    hum = F.col("humidity_pct")
    out_of_range = (
        (temp < TEMP_MIN_C) | (temp > TEMP_MAX_C) | (hum < HUMIDITY_MIN) | (hum > HUMIDITY_MAX)
    )
    return df.observe(
        name,
        # --- data-quality metrics ---
        F.count(F.lit(1)).alias("row_count"),
        F.sum(temp.isNull().cast("int")).alias("null_temp_count"),
        F.sum(hum.isNull().cast("int")).alias("null_humidity_count"),
        F.sum(out_of_range.cast("int")).alias("out_of_range_count"),
        F.collect_set("device_id").alias("devices_captured"),
        # --- value aggregates ---
        F.round(F.avg(temp), 2).alias("avg_temp_c"),
        F.round(F.min(temp), 2).alias("min_temp_c"),
        F.round(F.max(temp), 2).alias("max_temp_c"),
        F.round(F.avg(hum), 2).alias("avg_humidity_pct"),
        F.round(F.avg("battery_pct"), 2).alias("avg_battery_pct"),
    )

# COMMAND ----------


def start_fleet(spark: SparkSession, fleet: str):
    """Start the bronze + aggregation streams for one fleet; return the queries."""
    # --- bronze: raw parquet -> delta ---
    bronze_stream = (
        spark.readStream
        .schema(RAW_SCHEMA)
        .format("parquet")
        .load(landing_path(fleet))
    )

    bronze_query = (
        bronze_stream.writeStream.queryName(f"bronze_{fleet}")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path(f"bronze_{fleet}"))
        .trigger(availableNow=True)
        .toTable(bronze_table(fleet))
    )
    bronze_query.awaitTermination()

    # --- aggregation: bronze -> windowed metrics, with the observation attached ---
    # Lowering maxFilesPerBatch to demonstrate the query listener across batches.
    df_stream = spark.readStream.option("maxFilesPerBatch", "1").table(bronze_table(fleet))

    df_observation = with_iot_observation(df_stream, name="iot_metrics")

    agg = (
        df_observation
        .withWatermark("reading_ts", "10 minutes")
        .groupBy(
            F.window("reading_ts", "5 minutes").alias("window"),
            F.col("device_id"),
        )
        .agg(
            F.avg("temperature_c").alias("avg_temp_c"),
            F.max("temperature_c").alias("max_temp_c"),
            F.avg("humidity_pct").alias("avg_humidity_pct"),
            F.count(F.lit(1)).alias("reading_count"),
        )
    )
    agg_query = (
        agg.writeStream.queryName(f"agg_{fleet}")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path(f"agg_{fleet}"))
        .trigger(availableNow=True)
        .toTable(agg_table(fleet))
    )
    return [bronze_query, agg_query]

# COMMAND ----------

# Grant the service principal the permissions it needs on the target table.
spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{CLIENT_ID}`").collect()
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{CLIENT_ID}`").collect()
spark.sql(f"GRANT MODIFY, SELECT ON TABLE {METRICS_TABLE} TO `{CLIENT_ID}`").collect()

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# Register the reusable listener ONCE; it captures every query below. All values
# come from example_config.yaml; the secret comes from the secret scope.
config = ZerobusConfig(
    server_endpoint=ZB["server_endpoint"],
    workspace_url=ZB["workspace_url"],
    table_name=METRICS_TABLE,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
)

listener = ZerobusStreamingQueryListener(config)
spark.streams.addListener(listener)
print(f"Registered ZerobusStreamingQueryListener -> {METRICS_TABLE}")

streams = []
for fleet in FLEETS:
    streams.append(start_fleet(spark, fleet))

streams

# COMMAND ----------

# Wait for every query (availableNow), not awaitAnyTermination() which returns
# after just the first one ends.
queries = [q for fleet_queries in streams for q in fleet_queries]
for q in queries:
    q.awaitTermination()

# Listener callbacks are async: let all events drain before closing, or the
# final progress events are lost.
listener.wait_until_drained(expected_queries=len(queries))
listener.close()


# COMMAND ----------

display(spark.table(METRICS_TABLE))
