# Streaming Query Metrics → Delta via Databricks Zerobus

Capture Spark **Structured Streaming** query metrics with a `StreamingQueryListener` and
stream them straight into a **Delta table in Unity Catalog** using
**[Databricks Zerobus](https://docs.databricks.com/aws/ingestion/zerobus-ingest)**

## Why do this?

The [Databricks docs](https://docs.databricks.com/aws/en/structured-streaming/stream-monitoring#push-structured-streaming-metrics-to-external-services)
show how to push `StreamingQueryListener` metrics to **external** services (e.g. Kafka) for
alerting and dashboards. That works, but it means standing up and securing infrastructure
*outside* the platform your data already lives in.

This pattern keeps everything **inside Databricks**: the listener writes to a Delta table
in Unity Catalog via Zerobus. You get the same alerting/dashboarding, plus:

- **No external infra** — no Kafka/Prometheus to run, secure, or pay for.
- **UC-native** — metrics are a governed Delta table: SQL, AI/BI dashboards, lineage, ACLs.
- **Schema-stable** — open-ended progress payloads land in `VARIANT`, so the table never
  needs migrating as they evolve.
- **Low listener overhead** — the listener just hands JSON to a Zerobus sink (the docs warn
  that heavy listener logic slows queries).

## The core pattern (reusable)

Create the target table once (DDL lives in `streaming_listener.schema`):

```python
from streaming_listener.schema import create_table_ddl
spark.sql(create_table_ddl("<catalog>.<schema>.query_metrics"))
```

Then register one listener in your Spark session; every streaming query it runs writes its
metrics to that Delta table:

```python
from streaming_listener import ZerobusConfig, ZerobusStreamingQueryListener

config = ZerobusConfig(
    server_endpoint="https://<workspace-id>.zerobus.<region>.cloud.databricks.com",
    workspace_url="https://<deployment>.cloud.databricks.com",
    table_name="<catalog>.<schema>.query_metrics",
    client_id="<service-principal-client-id>",
    client_secret="<service-principal-client-secret>",
)
# or: ZerobusConfig.from_env(table_name="...")  # reads ZEROBUS_* env vars / secrets

spark.streams.addListener(ZerobusStreamingQueryListener(config))

# Create a readStream and results are sent to the metrics table
```

### Metrics table schema

Typed columns for common KPIs; `VARIANT` for the columns with varying complex fields.

| Column | Type | Notes |
|--------|------|-------|
| `event_type` | STRING | `started` / `progress` / `terminated` |
| `query_id`, `run_id`, `query_name` | STRING | Identity |
| `event_timestamp` | STRING | ISO-8601 from Spark |
| `batch_id`, `batch_duration_ms`, `num_input_rows` | BIGINT | Per-batch KPIs |
| `input_rows_per_second`, `processed_rows_per_second` | DOUBLE | Throughput |
| `duration_ms`, `event_time`, `state_operators`, `sources`, `sink`, `observed_metrics` | **VARIANT** | JSON payloads (per-phase timings, event-time/watermark, stateful-operator metrics, source/sink progress, custom `df.observe`) |
| `error_message`, `error_class` | STRING | Terminated events |

Zerobus maps Delta `VARIANT` to a JSON string on the wire, so the listener sends these
fields as JSON with `RecordType.JSON` (no proto-compile step).

## Repo Layout

| Path | What it is |
|------|-----------|
| `src/streaming_listener/` | **Core reusable pattern** — the listener, Zerobus sink, config, and metrics-table schema. Packaged as a wheel. |
| `example/` | Self-contained IoT example + Databricks Asset Bundle (pipeline notebooks, config, dashboard, and `databricks.yml`). See [`example/README.md`](example/README.md). |

## Example

A complete, runnable example lives under [`example/`](example/README.md)

## Tooling

- **uv** for Python: `uv build --wheel`, `uv run ...`.
- **ruff** for lint/format: `uvx ruff format . && uvx ruff check .`.
- **basedpyright** for types: `uvx basedpyright src example`.
