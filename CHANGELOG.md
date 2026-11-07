# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-11-08

### Added

- A legacy estate of ten flows across nine systems, with fixed-width layouts and sample inputs.
- Interface inventory: complexity scores, tiers, dependency waves and a dependency graph.
- Jinja2 templates generating topics, JSON Schemas, ingest and deliver specs and runbooks, with a
  drift check.
- The Kafka runtime: strict transforms, dead-letter topics, control messages and complete-batch
  delivery.
- The regression harness with a ledger of accepted differences, and the `imf` command.
