# Architecture

## Components

| Module | Responsibility |
|---|---|
| `model.py` | Parse and validate the estate: flow XML, fixed-width layouts. Unknown transforms, unjoined multi-field maps, keys missing from the map, unknown layouts or dependencies, overlapping layout fields are refused at load time. |
| `inventory.py` | Complexity score per flow (a weighted sum documented in the module), tiers, dependency waves with cycle detection, Mermaid graph, CSV and Markdown output. |
| `generate.py` + `templates/` | Render the per-flow artifacts. Field types (decimal with scale, integer, date pattern, string) come from the transform pipeline or the source layout and drive the JSON Schema. |
| `transforms.py` | The new platform's transforms: exact decimals, half-up rounding, strict parsing that raises `Rejected(reason)`. |
| `formats.py` | Source readers (CSV, fixed-width with implied decimals, XML) and target writers (CSV, fixed-width, JSON Lines). |
| `runtime.py` | Ingest and deliver, over a `Bus`: Kafka (idempotent producer, whole-topic reads) or in-memory for tests. |
| `legacy.py` | The legacy engine's behaviour, quirks included, to produce golden outputs. |
| `harness.py` | Keyed comparison, DLQ matching, accepted-difference rules and limits, control totals, report. |
| `cli.py` | `imf inventory`, `generate`, `samples`, `regress`, `compare`, `run`, `topics`. |

## Event contract

One topic per flow, `<domain>.<flow-id>.v1`, keyed by the flow key. The value is the mapped record
as JSON: decimals as strings so no precision is lost, integers as numbers, dates in the target's
format. Headers carry `batch_id` and `seq` (the record's line in the source file).

Each flow has a DLQ, `<topic>.dlq`, whose messages carry the reason (`invalid_date`,
`invalid_decimal`, `invalid_integer`, `malformed_row`, `schema_violation`, `invalid_filter_value`),
the detail, the best-effort key and the original source line.

`imf.control.v1` receives one message per batch: records published, read, filtered, rejected by
reason, and the sum of every decimal field.

## Batch completeness

Files are batches, and a target system must never load half of one. Ingest flushes all records
before it publishes the control message. Deliver reads the batch, removes duplicates by `seq`, and
writes the file only when the number of records and the control totals equal the control message.
Otherwise it raises `IncompleteBatch` and writes nothing. Records are written in source order, so the
target file matches the legacy file line for line, even though the topic has three partitions.

## Regression

For every flow:

1. The legacy engine maps the sample input to the golden output (committed, and reproducible:
   `imf samples --check`).
2. Ingest and deliver produce the new output from the same input.
3. Records are matched on the flow key. A legacy record with no new counterpart is matched to its
   dead letter; a field that differs is compared again under each accepted rule.
4. A flow fails on any unexplained difference, any rule used more than its `max_records`, or
   missing or extra records.

## Deployment shape

The generated `topics.yaml` asks for three partitions, replication 3 and `min.insync.replicas=2`;
retention follows the data classification (7 days for confidential, 14 for internal). `imf topics`
creates them, capping replication at the cluster's broker count for development. Ingest and deliver
are short-lived jobs, scheduled where the legacy schedule ran; the `consumer_group` in each deliver
spec is reserved for a long-running variant that commits offsets.
