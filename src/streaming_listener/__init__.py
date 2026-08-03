"""Reusable pattern: capture Spark streaming query metrics via Databricks Zerobus.

The two things you need from this package:

    from streaming_listener import ZerobusConfig, ZerobusStreamingQueryListener

    listener = ZerobusStreamingQueryListener(ZerobusConfig(...))
    spark.streams.addListener(listener)

Every streaming query running in that Spark session will then have its
``onQueryStarted`` / ``onQueryProgress`` / ``onQueryTerminated`` events written
to a single Delta table in Unity Catalog, with sink/source/duration/custom
observation payloads landing in ``variant`` columns.
"""

from streaming_listener.config import ZerobusConfig
from streaming_listener.listener import ZerobusStreamingQueryListener
from streaming_listener.sink import ZerobusSink

__all__ = [
    "ZerobusConfig",
    "ZerobusStreamingQueryListener",
    "ZerobusSink",
]
