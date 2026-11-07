# 1. Generate every flow from templates, and commit the output

**Status:** accepted

## Context

Hand-migrating hundreds of flows produces hundreds of slightly different implementations. Reviewers
cannot see what changed between the legacy definition and the new one, and fixing a pattern means
finding every copy of it.

## Decision

Each flow's Kafka artifacts (topics, schema, ingest and deliver specs, runbook) are rendered from one
set of Jinja2 templates. The runtime reads only the rendered specs. The rendered files are committed
under `generated/`, and CI runs `imf generate --check`, which fails if they differ from what the
estate and templates produce now, or if a file exists that no flow generates.

## Consequences

- A template fix is one change, and the diff in `generated/` shows exactly which flows it affects.
- What will be deployed is visible in review, not computed at deploy time.
- Flows that need behaviour the templates cannot express need a template feature, not a one-off edit.
  Hand edits to `generated/` fail CI.
