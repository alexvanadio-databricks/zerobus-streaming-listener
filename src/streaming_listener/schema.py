"""DDL for the streaming-query metrics table (the Zerobus target).

Hybrid schema: common KPIs are typed columns for cheap, index-friendly
dashboarding; the open-ended parts of a progress event are ``VARIANT`` so we can
store arbitrary sink/source/observation payloads without schema churn.

Zerobus maps Delta ``VARIANT`` to a JSON string on the wire, so the listener
sends these fields as JSON and they are parsed into ``VARIANT`` on write.
"""

from __future__ import annotations

# Columns, in table order. Kept as data so the DDL and any docs stay in sync.
COLUMNS: list[tuple[str, str, str]] = [
    ("event_type", "STRING", "started | progress | terminated"),
    ("query_id", "STRING", "Stable streaming query id"),
    ("run_id", "STRING", "Id of this particular run of the query"),
    ("query_name", "STRING", "queryName() of the stream"),
    ("event_timestamp", "STRING", "Event timestamp (ISO-8601 from Spark)"),
    ("batch_id", "BIGINT", "Micro-batch id (progress events)"),
    ("batch_duration_ms", "BIGINT", "Wall-clock duration of the batch"),
    ("num_input_rows", "BIGINT", "Rows read in the batch"),
    ("input_rows_per_second", "DOUBLE", "Input throughput"),
    ("processed_rows_per_second", "DOUBLE", "Processing throughput"),
    ("duration_ms", "VARIANT", "Per-phase timings map (variant)"),
    ("event_time", "VARIANT", "Event-time avg/max/min/watermark map (variant)"),
    ("state_operators", "VARIANT", "Stateful-operator metrics array (variant)"),
    ("sources", "VARIANT", "Source progress array (variant)"),
    ("sink", "VARIANT", "Sink progress (variant)"),
    ("observed_metrics", "VARIANT", "Custom df.observe metrics (variant)"),
    ("error_message", "STRING", "Exception text (terminated events)"),
    ("error_class", "STRING", "Error class (terminated events)"),
]


def create_table_ddl(table_name: str) -> str:
    """Return ``CREATE TABLE IF NOT EXISTS`` DDL for the metrics table."""
    cols = ",\n    ".join(f"{name} {sql_type}" for name, sql_type, _ in COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {cols}\n)\n"
        "USING DELTA\n"
    )
