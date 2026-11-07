# 3. Reject unparseable values to a DLQ instead of reproducing legacy defaults

**Status:** accepted

## Context

The legacy engine never rejects a record. An impossible date becomes a blank, an unparseable number
becomes zero. Reproducing that keeps the outputs identical, and keeps shipping orders worth 0.00.

## Decision

The new transforms are strict. A value that cannot be converted raises a reason, and the record goes
to the flow's DLQ with the original source line. Each such case is then an accepted difference
(AD-001, AD-002), approved by the flow owner.

Where the legacy behaviour is a constraint of the target rather than a defect, it is kept: values
longer than a fixed-width field are truncated on both platforms, and the harness reports how many.

## Consequences

- Bad source data surfaces where it can be fixed, with a reason, instead of downstream as a wrong
  value.
- Targets receive fewer records than before when the source is bad; the regression report and the
  control message say exactly how many and why.
