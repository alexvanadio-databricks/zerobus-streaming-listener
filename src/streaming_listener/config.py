"""Configuration for the Zerobus streaming-query listener.

Keeping this in one small dataclass is what makes the pattern "simple to
configure": construct one ``ZerobusConfig`` and hand it to the listener.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ZerobusConfig:
    """Everything the listener needs to open a Zerobus ingest stream.

    Attributes:
        server_endpoint: Zerobus gRPC endpoint, e.g.
            ``https://<workspace-id>.zerobus.<region>.cloud.databricks.com``.
        workspace_url: Unity Catalog / workspace URL, e.g.
            ``https://<deployment>.cloud.databricks.com``.
        table_name: Fully-qualified target table, e.g.
            ``main.streaming_monitor.query_metrics``.
        client_id: Service-principal OAuth client id.
        client_secret: Service-principal OAuth client secret.
    """

    server_endpoint: str
    workspace_url: str
    table_name: str
    client_id: str
    client_secret: str

    @classmethod
    def from_env(cls, table_name: str | None = None) -> ZerobusConfig:
        """Build config from environment variables / Databricks secrets.

        Reads ``ZEROBUS_SERVER_ENDPOINT``, ``ZEROBUS_WORKSPACE_URL``,
        ``ZEROBUS_TABLE_NAME`` (unless ``table_name`` is passed),
        ``ZEROBUS_CLIENT_ID`` and ``ZEROBUS_CLIENT_SECRET``. On a Databricks
        job these are typically injected from a secret scope via the task
        environment.
        """
        resolved_table = table_name or _require_env("ZEROBUS_TABLE_NAME")
        return cls(
            server_endpoint=_require_env("ZEROBUS_SERVER_ENDPOINT"),
            workspace_url=_require_env("ZEROBUS_WORKSPACE_URL"),
            table_name=resolved_table,
            client_id=_require_env("ZEROBUS_CLIENT_ID"),
            client_secret=_require_env("ZEROBUS_CLIENT_SECRET"),
        )


    def _require_env(name: str) -> str:
        value = os.environ.get(name)
        if not value:
            raise ValueError(f"Required environment variable {name!r} is not set")
        return value
