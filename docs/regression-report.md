# Regression report

Each flow's sample input was run through the legacy engine (golden output) and through the Kafka path (Kafka). Outputs are compared record by record on the flow key. A flow passes when every difference is accepted in `accepted_differences.yaml` within its limit.

**10 of 10 flows pass.**

| Flow | Legacy records | New records | Rejected to DLQ | Differences | Accepted | Unexplained | Order | Result |
|---|---|---|---|---|---|---|---|---|
| CUST-001 | 300 | 293 | invalid_date 7 | 22 | 22 | 0 | same | pass |
| INV-001 | 300 | 300 | 0 | 0 | 0 | 0 | same | pass |
| INV-002 | 300 | 294 | invalid_date 6 | 300 | 300 | 0 | same | pass |
| INVC-001 | 215 | 215 | 0 | 7 | 7 | 0 | same | pass |
| ORD-001 | 272 | 264 | invalid_date 5, invalid_decimal 3 | 17 | 17 | 0 | same | pass |
| ORD-002 | 287 | 287 | 0 | 0 | 0 | 0 | same | pass |
| PAY-001 | 300 | 300 | 0 | 0 | 0 | 0 | same | pass |
| PRC-001 | 120 | 120 | 0 | 0 | 0 | 0 | same | pass |
| RET-001 | 114 | 114 | 0 | 0 | 0 | 0 | same | pass |
| SUP-001 | 150 | 150 | 0 | 0 | 0 | 0 | same | pass |

## Control totals

| Flow | Field | Legacy | New | Difference |
|---|---|---|---|---|
| INVC-001 | net | 546819.46 | 546819.46 | 0.00 |
| INVC-001 | gross | 645246.96 | 645247.03 | 0.07 |
| ORD-001 | amount | 3401973.73 | 3320923.55 | -81050.18 |
| ORD-002 | unit_price | 129184.58 | 129184.58 | 0.00 |
| PAY-001 | amount | 750839.77 | 750839.77 | 0.00 |
| PRC-001 | list_price | 50582.64 | 50582.64 | 0.00 |
| PRC-001 | promo_price | 13595.22 | 13595.22 | 0.00 |

Totals differ only by the records rejected to the DLQ and the accepted rounding corrections.

## Differences by accepted rule

| Rule | Flows | Records | Limit per flow | Rationale | Ticket |
|---|---|---|---|---|---|
| AD-001 (rejected) | CUST-001, ORD-001, INV-002 | 18 | 10 | The legacy engine writes a blank date when the source date does not exist (31 February), and the targets load a null. The record is rejected to the DLQ so the owner corrects it at source. | MIG-104 |
| AD-002 (rejected) | ORD-001 | 3 | 5 | A corrupted amount field is written as 0.00 by the legacy engine, which ships an order with a zero value to the warehouse. It is rejected instead. | MIG-105 |
| AD-003 (unicode_case) | CUST-001, ORD-001 | 24 | 40 | Legacy upper-casing leaves non-ASCII letters alone ("FEMI åNGSTRöM"). The new platform upper-cases them correctly; CRM and WMS both store UTF-8 and were tested with the new values. | MIG-111 |
| AD-004 (tolerance) | INVC-001 | 7 | 15 | Legacy computes gross = net x 1.18 in binary floating point, so a result exactly on half a cent can round down. The new platform uses exact decimal arithmetic and rounds half up, as the finance policy requires. Finance confirmed the general ledger tolerates one-cent corrections. | MIG-118 |
| AD-005 (typed_json) | INV-002 | 294 | 1000 | The legacy JSON writer emits every value as a string. The BI loader casts quantity to an integer either way; the typed value matches the published schema. | MIG-120 |

## Examples

- CUST-001 `C000007` name: legacy `"DANA JOSé PEñA"`, new `"DANA JOSÉ PEÑA"` (AD-003).
- CUST-001 `C000013`: legacy wrote it; rejected as `invalid_date` (AD-001).
- INV-002 `MV0000000` quantity: legacy `"28"`, new `28` (AD-005).
- INVC-001 `INV000000020` gross: legacy `"1031.02"`, new `"1031.03"` (AD-004).
- ORD-001 `ORD0000041`: legacy wrote it; rejected as `invalid_decimal` (AD-002).

## Data loss carried over for compatibility

- INV-001: 11 `description` values longer than the fixed-width target field are truncated, as the legacy flow does. Both platforms agree; the target layout is the constraint.
