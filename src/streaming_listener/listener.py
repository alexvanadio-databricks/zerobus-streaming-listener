"""A ``StreamingQueryListener`` that writes query metrics to Delta via Zerobus.

One listener instance handles every streaming query in the Spark session, so
multiple streams all log to the same target table (see the example pipeline).

The rows it produces match the hybrid schema in ``schema.py``: common KPIs are
top-level typed columns, while the open-ended parts of a progress event
(``durationMs``, ``sources``, ``sink``, and any custom ``df.observe`` metrics)
are serialized to JSON strings and land in ``variant`` columns.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from typing import Any

from pyspark.sql.streaming.listener import (
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
    StreamingQueryListener,
)

from streaming_listener.config import ZerobusConfig
from streaming_listener.sink import ZerobusSink

logger = logging.getLogger(__name__)


def _to_jsonable(obj: Any) -> Any:
    """Best-effort convert an SDK event object into JSON-serializable data."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(v) for v in obj]
    if hasattr(obj, "__dict__"):
        return {k: _to_jsonable(v) for k, v in vars(obj).items()}
    return str(obj)


def _json_or_none(obj: Any) -> str | None:
    """Serialize to a JSON string for a ``variant`` column, or None if empty."""
    data = _to_jsonable(obj)
    if data is None or data == {} or data == []:
        return None
    return json.dumps(data)


@dataclass
class _RunState:
    """Per-query context remembered across events, keyed by runId.

    ``QueryTerminatedEvent`` exposes only ids and error fields, so we carry the
    query name and the most recent event timestamp forward from the started /
    progress events to enrich the terminated log row.
    """

    name: str | None = None
    last_timestamp: str | None = None


class ZerobusStreamingQueryListener(StreamingQueryListener):
    """Streams query lifecycle + progress metrics to a Delta table via Zerobus.

    Args:
        config: Zerobus connection + target-table configuration.
        sink: Optional pre-built sink (used in tests); one is created from
            ``config`` otherwise.
    """

    def __init__(self, config: ZerobusConfig, sink: ZerobusSink | None = None):
        super().__init__()
        self._sink = sink or ZerobusSink(config)
        # One listener serves every query in the session, so per-query state is
        # keyed by runId. QueryTerminatedEvent carries neither the query name nor
        # a timestamp, so we remember them from the started/progress events and
        # look them up when the query terminates.
        self._runs: dict[str, _RunState] = {}
        self._runs_lock = threading.Lock()

    # --- lifecycle events -------------------------------------------------

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        with self._runs_lock:
            self._runs[str(event.runId)] = _RunState(name=event.name, last_timestamp=event.timestamp)

        self._safe_ingest(
            {
                "event_type": "started",
                "query_id": str(event.id),
                "run_id": str(event.runId),
                "query_name": event.name,
                "event_timestamp": event.timestamp,
            }
        )

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        p = event.progress
        # Custom df.observe(...) metrics show up here as observedMetrics.
        observed = getattr(p, "observedMetrics", None)
        with self._runs_lock:
            state = self._runs.setdefault(str(p.runId), _RunState(name=p.name))
            state.name = p.name
            state.last_timestamp = p.timestamp

        self._safe_ingest(
            {
                "event_type": "progress",
                "query_id": str(p.id),
                "run_id": str(p.runId),
                "query_name": p.name,
                "event_timestamp": p.timestamp,
                "batch_id": p.batchId,
                "batch_duration_ms": p.batchDuration,
                "num_input_rows": p.numInputRows,
                "input_rows_per_second": p.inputRowsPerSecond,
                "processed_rows_per_second": p.processedRowsPerSecond,
                # variant columns (JSON strings):
                "duration_ms": _json_or_none(p.durationMs),
                "event_time": _json_or_none(getattr(p, "eventTime", None)),
                "state_operators": _json_or_none(getattr(p, "stateOperators", None)),
                "sources": _json_or_none(p.sources),
                "sink": _json_or_none(p.sink),
                "observed_metrics": _json_or_none(observed),
            }
        )

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        # The event has no name/timestamp; recover them from the tracked run.
        with self._runs_lock:
            state = self._runs.pop(str(event.runId), None)

        self._safe_ingest(
            {
                "event_type": "terminated",
                "query_id": str(event.id),
                "run_id": str(event.runId),
                "query_name": state.name if state else None,
                "event_timestamp": state.last_timestamp if state else None,
                "error_message": event.exception,
                "error_class": event.errorClassOnException,
            }
        )

    # --- helpers ----------------------------------------------------------

    def _safe_ingest(self, record: dict[str, Any]) -> None:
        """Never let a listener failure break the streaming query."""
        try:
            self._sink.ingest(record)
        except Exception as e:  # noqa: BLE001 - listener must not raise
            print(str(e))
            logger.exception("Failed to ingest streaming query event to Zerobus")

    def close(self) -> None:
        """Flush and close the underlying sink."""
        self._sink.close()
