# 2. Prove equivalence against golden files, with a ledger of accepted differences

**Status:** accepted

## Context

"The new flow works" usually means it ran and the target accepted the file. That says nothing about
whether the content is what the legacy platform would have produced. Some differences are wanted (a
corrupt amount should not ship as 0.00), most are not, and the two look the same in a diff.

## Decision

Every flow is compared, record by record and field by field, with the legacy output for the same
input. Every difference must match a rule in `accepted_differences.yaml`. Each rule states the flows
and fields it covers, why it is acceptable, who approved it, the ticket, and the most records it may
explain in one flow. The harness fails on anything else.

## Consequences

- Behavioural changes are decisions with owners, visible in review and in the report.
- `max_records` stops an accepted rule from hiding a new problem of the same shape.
- Tolerance rules are narrow (one field, an absolute tolerance), so a one-cent rounding correction
  cannot hide a five-cent error.
- Golden files must be captured from production for a real migration; the samples here exercise the
  quirks, they do not replace production data.
