"""Thin wrapper around the Zerobus ingest SDK.

The listener does not talk to the SDK directly; it talks to this sink. That
keeps the SDK's connection lifecycle (lazy open, flush, close) in one place and
makes the listener trivial to unit-test with a fake sink.

We use ``RecordType.JSON`` deliberately:

* No proto-compile step, so the pattern stays "simple to configure".
* Delta ``VARIANT`` columns map to a protobuf/JSON string of JSON, so we can
  send nested dicts (sink, sources, durationMs, observed metrics) as JSON and
  they land in the ``variant`` columns unchanged.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from streaming_listener.config import ZerobusConfig

logger = logging.getLogger(__name__)


class ZerobusSink:
    """Opens a Zerobus stream on first use and ingests dict records as JSON."""

    def __init__(self, config: ZerobusConfig):
        self._config = config
        self._stream: Any = None
        self._sdk: Any = None
        self._lock = threading.Lock()

    def _ensure_stream(self) -> Any:
        """Lazily create the SDK + stream.

        Done lazily (not in ``__init__``) so the listener can be constructed on
        the driver before the SDK/network is touched, and so a construction-time
        import error doesn't take down query registration.
        """
        if self._stream is not None:
            return self._stream

        with self._lock:
            if self._stream is not None:
                return self._stream

            # Imported here so the module can be imported (and unit-tested with
            # a fake sink) on machines without the SDK installed.
            from zerobus.sdk.shared import (
                RecordType,
                StreamConfigurationOptions,
                TableProperties,
            )
            from zerobus.sdk.sync import ZerobusSdk

            self._sdk = ZerobusSdk(
                self._config.server_endpoint,
                self._config.workspace_url,
            )
            table_properties = TableProperties(self._config.table_name)
            options = StreamConfigurationOptions(record_type=RecordType.JSON)
            self._stream = self._sdk.create_stream(
                self._config.client_id,
                self._config.client_secret,
                table_properties,
                options,
            )
            logger.info("Opened Zerobus stream to %s", self._config.table_name)

        return self._stream

    def ingest(self, record: dict[str, Any]) -> None:
        """Queue a single record for ingestion (non-blocking on the server)."""
        stream = self._ensure_stream()
        stream.ingest_record_offset(record)

    def flush(self) -> None:
        """Block until all queued records are durably committed."""
        if self._stream is not None:
            self._stream.flush()

    def close(self) -> None:
        """Flush and close the stream. Safe to call more than once."""
        with self._lock:
            if self._stream is None:
                return
            try:
                self._stream.flush()
            finally:
                self._stream.close()
                self._stream = None
                logger.info("Closed Zerobus stream to %s", self._config.table_name)
