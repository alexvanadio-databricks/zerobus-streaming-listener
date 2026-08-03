# Example: IoT streaming pipeline + dashboard

A self-contained, runnable example of the [`streaming_listener`](../README.md) pattern,
packaged as a Databricks Asset Bundle. It runs two IoT device fleets as independent
streaming queries in one Spark session, both logging to the **same** metrics table through
a single registered listener.

The aggregation stream attaches one `df.observe("iot_metrics", ...)` (see
`with_iot_observation` in `pipeline.py`) that reports data-quality metrics (null counts,
out-of-range readings, devices captured) and value aggregates (avg/min/max temperature,
etc.). Spark surfaces these on each progress event, and the listener writes them to the
`observed_metrics` VARIANT column, which the dashboard reads.

## Layout

| Path | What it is |
|------|-----------|
| `example_config.yaml` | All settings: catalog, schema, fleets, Zerobus endpoint, client id, and the secret scope for the client secret. |
| `setup_resources.py` | Creates the schema, landing volume, and metrics table. Run once. |
| `generate_data.py` | `dbldatagen` writes fake IoT telemetry (two fleets) as parquet into the volume. |
| `pipeline.py` | Registers the listener and runs the bronze + windowed-aggregation streams. |
| `dashboard/streaming_health.lvdash.json` | AI/BI (Lakeview) dashboard over the metrics table. |
| `resources/dashboard.yml` | Bundle dashboard resource. |
| `databricks.yml` | Bundle definition + variables (dashboard-only deploy). |

## Configure

Everything comes from `example_config.yaml`. The Zerobus **client secret is never stored
here** — it is read at runtime from the secret scope named in the file:

```bash
databricks secrets create-scope zerobus
databricks secrets put-secret zerobus client_secret
```

## Run the pipeline

Run the notebooks on a cluster, in order:

1. `setup_resources.py` — create the schema, volume, and metrics table.
2. `generate_data.py` — land fake telemetry into the volume.
3. `pipeline.py` — start the streams; both fleets log to the metrics table via the listener.

## Deploy the dashboard (DAB)

The bundle deploys only the AI/BI dashboard. Set `warehouse_id` and the workspace `host`
in `databricks.yml`, then, from this `example/` directory:

```bash
databricks bundle validate
databricks bundle deploy
```

The dashboard datasets query the metrics table, so it must exist (and have data) first —
run `pipeline.py` above to populate it. If you change the catalog/schema in
`example_config.yaml`, also update the `FROM` clauses in
`dashboard/streaming_health.lvdash.json`.
