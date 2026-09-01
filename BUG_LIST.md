# Bug List

Defects found in the example while getting the `StreamingQueryListener` metrics
to actually land in the Zerobus table, with fixes. Verified against a live
workspace on DBR 17.3 LTS.

---

## 1. Progress events rejected — non-finite floats produce invalid JSON

Spark reports `inputRowsPerSecond` / `processedRowsPerSecond` as `Infinity`/`NaN`
for zero-duration batches (the first micro-batch almost always is). `json.dumps`
emits a literal `Infinity`/`NaN`, which is invalid JSON, so Zerobus rejects the
**entire progress record** (error 4044). Lifecycle events carry no float fields,
so `started`/`terminated` land while **every `progress` event silently
disappears** — losing all throughput and `df.observe` metrics. Isolation-tested
straight through the SDK.

**Fix** (`src/streaming_listener/listener.py`): `_finite_or_none()` coerces
non-finite floats to `None`; applied to both rps fields.

---

## 2. `df.observe` aliases dropped — observed metrics serialized positionally

`observed_metrics` landed as a positional array (`[150000, 2964, ...]`) instead of
keyed by the observation aliases. Cause: `progress.observedMetrics` is
`{name: Row}`, and a pyspark `Row` is a subclass of `tuple`, so the JSON helper
iterated it positionally and discarded the field names.

**Fix** (`listener.py`): `_to_jsonable()` handles a `Row` (via `asDict()`) before
the list/tuple branch, so metrics serialize as `{"row_count": 150000, ...}`. Also
protects any other Row-shaped payloads (source/sink progress).

---

## 3. Final progress events not durably written — no flush/close + async race

Two issues in `notebooks/pipeline.py`:

1. It used `spark.streams.awaitAnyTermination()` (returns after the *first* query
   terminates) and never closed the listener/sink, so durability of the last
   records relied on the SDK's background auto-flush.
2. `StreamingQueryListener` callbacks are asynchronous; closing the sink
   immediately after `awaitTermination()` can tear it down while a progress
   event's ingest is still in flight. A/B confirmed: a short sleep before close →
   progress lands; no sleep → the final progress event is lost.

**Fix** (`pipeline.py`): await **every** query, then
`listener.wait_until_drained(...)` (blocks until all terminated events are
delivered; events are ordered, so all progress is delivered by then), then
`listener.close()` (flush + close).
