# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [0.1.1] - 2026-11-08

### Fixed

- Deliver waits up to 30 s for a batch's records to become visible before refusing it. On a broker that had just
  started, four records of a batch were not yet readable when deliver ran; the control message caught it and no
  partial file was written, but the run failed.
- The Kafka bus waits for a new topic's partitions to have leaders before sending to it, and a send the broker
  refuses now fails the flush instead of disappearing from the batch.

## [0.1.0] - 2026-11-08

### Added

- A legacy estate of ten flows across nine systems, with fixed-width layouts and sample inputs.
- Interface inventory: complexity scores, tiers, dependency waves and a dependency graph.
- Jinja2 templates generating topics, JSON Schemas, ingest and deliver specs and runbooks, with a
  drift check.
- The Kafka runtime: strict transforms, dead-letter topics, control messages and complete-batch
  delivery.
- The regression harness with a ledger of accepted differences, and the `imf` command.
