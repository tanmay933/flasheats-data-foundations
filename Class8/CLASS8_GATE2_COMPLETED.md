# Gate 2 — Data Readiness Review

## Pipeline run

- **One-command flow:** PASS — the pipeline completed end-to-end for `run_date=2026-09-22`.
- **Safe rerun:** PASS — the same logical output partition is atomically replaced rather than appended.
- **Raw API preservation:** PASS — every Dispatch page is written before downstream transformation.
- **Publish after validation:** PASS — validation errors stop the pipeline before processed output is written.

## Validation

| Check | Status | Evidence / note |
|---|---|---|
| Required columns | PASS | All required order and Dispatch fields were present in the normal run. `missing_column` fails with `orders: missing required columns: ['promised_eta']`. |
| Critical nulls | WARN | 37 delivered rows lack `actual_delivery_at`; these cannot support delivery-delay measurement. |
| Order uniqueness | WARN | Source contains duplicate rows; normal run reports 6 duplicate rows involved and the cleaning stage reduces 1603 rows to 1600 orders. |
| Freshness | PASS | Latest `created_at` = `2026-08-28 23:33:00`; for run date `2026-09-22`, age is 25 days and the configured maximum is 60 days. |
| Dispatch retrieval completeness | PASS | 8 pages retrieved; 1,600 records received and expected total was 1,600. |

## Reliability

| Capability | Status | Evidence / note |
|---|---|---|
| Bounded retries | PASS | HTTP 500 on page 3 and HTTP 429 on page 5 were retried within the configured maximum. |
| Useful failure message | PASS | Missing schema and stale-data scenarios fail with specific validation messages. |
| Logging | PASS | Logs include stage, page, status, attempt, record counts, validation results, and publication outcome. |
| Idempotent rerun | PASS | The same `run_date` uses the same output partition and atomic replacement. |
| Configuration outside core logic | PASS | API URL, retry limits, freshness limit, page size, and log level are environment-configurable. |

## Controlled failure results

- `missing_column` → **FAIL safely** with no processed output publication.
- `duplicate_order` → **WARN then clean**, resulting in one row per order.
- `stale_data` → **FAIL safely** because the data is 390 days old versus a 60-day limit.

## Gate decision

**READY** for the scope of this classroom pipeline, with the known data-quality warnings documented above.

The pipeline is suitable for the next product/AI phase only with the stated limitations understood: missing delivery timestamps and source duplicates remain visible as validation warnings, and downstream consumers should use the measurable-delivery population for delay metrics.

> The readiness decision is about the reliability of this classroom pipeline, not a claim that the underlying business KPI is universally approved.
