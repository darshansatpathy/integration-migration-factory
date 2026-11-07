# 4. Carry file batches over Kafka with a control message

**Status:** accepted

## Context

The flows are file interfaces: the target expects a complete file, in order, once. Kafka delivers
records, possibly duplicated by retries, across partitions with no global order.

## Decision

Ingest tags every record with its batch id and source sequence and, after all records are
acknowledged, publishes a control message with the count and the control totals. Deliver writes the
target file only when the batch on the topic matches the control message, after removing duplicates
by sequence and sorting by it.

## Consequences

- A partial batch is never delivered, and a record changed in transit is detected by the totals.
- The output file is ordered as the legacy file was, so line-level comparisons stay meaningful.
- Deliver holds a whole batch in memory; that suits file sizes in this estate, and a streaming
  writer with offset commits is the path for very large files.
