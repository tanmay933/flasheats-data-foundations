"""
src/validation/validate.py

Validation layer for the FlashEats pipeline.

Inspects the raw records returned by src/ingestion/ingest.py and reports
data-quality issues as a structured validation report. This module never
modifies, cleans, deduplicates, or normalizes the input data -- it only
detects and describes problems. Fixing/normalizing happens later in
src/transformation.

Each validation function returns a list of "finding" dicts:

    {
        "source": str,             # e.g. "orders"
        "rule": str,                # e.g. "duplicate_order_id"
        "severity": "error" | "warning",
        "affected_count": int,
        "details": str,             # human-readable description
        "example_records": [...]    # small sample (<=3) for inspection
    }

Severity convention used throughout this module:
    - "error":   the issue directly undermines trustworthy metric
                 calculation for the affected records (e.g. missing
                 delivery timestamps means delivery time cannot be
                 computed at all) and must be handled explicitly
                 downstream (exclude / flag) rather than averaged over.
    - "warning": the issue is real and must be documented (per the
                 assignment's Known/Unknown/Assumption/Limitation
                 section) but the affected records can still be
                 retained for some metrics.
"""

import json
import logging
import re
from collections import Counter, defaultdict
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("validation")

MAX_EXAMPLES = 3

ORDER_REF_PATTERN = re.compile(r"^ORD-(\d+)$")
VALID_TICKET_CATEGORIES = {"late_delivery", "wrong_item", "refund", "other"}
CANONICAL_COURIER_ORDER = ["assigned", "picked_up", "en_route", "delivered"]


# --------------------------------------------------------------------------
# Small shared helpers (read-only / detection use only -- these never
# mutate or replace values in the source records)
# --------------------------------------------------------------------------

def _finding(source, rule, severity, affected_count, details, examples=None):
    return {
        "source": source,
        "rule": rule,
        "severity": severity,
        "affected_count": affected_count,
        "details": details,
        "example_records": (examples or [])[:MAX_EXAMPLES],
    }


def _is_blank(value):
    return value is None or (isinstance(value, str) and value.strip() == "")


def _parse_iso(ts):
    """Best-effort parse of an ISO-ish timestamp string like '...T12:31:00Z'. Returns None on failure."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.rstrip("Z"))
    except (ValueError, AttributeError):
        return None


def _parse_clock_time(ts):
    """Parse a bare 'HH:MM:SS' string (restaurant POS timestamps have no date/timezone). None on failure."""
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%H:%M:%S")
    except ValueError:
        return None


def _extract_order_ref_id(order_ref):
    """
    Detection-only helper: returns (numeric_id_or_None, is_well_formed).
    Well-formed means it matches the canonical 'ORD-<digits>' pattern.
    If not well-formed, falls back to a loose digit scan so malformed refs
    can still be grouped for cross-checks -- this does NOT alter the
    original record, it's purely internal to validation logic.
    """
    if not order_ref:
        return None, False
    m = ORDER_REF_PATTERN.match(order_ref)
    if m:
        return int(m.group(1)), True
    loose = re.search(r"(\d+)", order_ref)
    if loose:
        return int(loose.group(1)), False
    return None, False


def _restaurant_row_order_id_field(row):
    """Returns the raw order-id value from a restaurant row, whichever column name it used."""
    if "order_id" in row:
        return row.get("order_id")
    if "OrderID" in row:
        return row.get("OrderID")
    return None


def _restaurant_row_timestamps(row):
    """Returns (accepted_raw, ready_raw) whichever column names the row's schema used."""
    if "accepted_time" in row or "food_ready_time" in row:
        return row.get("accepted_time"), row.get("food_ready_time")
    if "Accepted_At" in row or "Ready_At" in row:
        return row.get("Accepted_At"), row.get("Ready_At")
    return None, None


# --------------------------------------------------------------------------
# 1. Orders DB validation
# --------------------------------------------------------------------------

REQUIRED_ORDER_FIELDS = ["customer_id", "restaurant_id", "order_placed_at", "promised_by", "payment_method", "status"]


def validate_orders(orders):
    findings = []

    # -- duplicate order_id --
    id_counts = Counter(o.get("order_id") for o in orders)
    duplicate_ids = {oid for oid, count in id_counts.items() if count > 1}
    if duplicate_ids:
        examples = [o for o in orders if o.get("order_id") in duplicate_ids][:MAX_EXAMPLES]
        extra_rows = sum(id_counts[oid] - 1 for oid in duplicate_ids)
        findings.append(_finding(
            "orders", "duplicate_order_id", "error",
            extra_rows,
            f"{len(duplicate_ids)} order_id value(s) appear more than once "
            f"({extra_rows} extra row(s) total) -- would double-count in any metric that sums/counts orders.",
            examples,
        ))

    # -- null order_total --
    null_total = [o for o in orders if o.get("order_total") is None]
    if null_total:
        findings.append(_finding(
            "orders", "null_order_total", "warning",
            len(null_total),
            "order_total is null -- these orders can still be used for timing/workflow metrics "
            "but must be excluded from any revenue-based metric.",
            null_total,
        ))

    # -- zero/negative order_total --
    bad_total = [o for o in orders if isinstance(o.get("order_total"), (int, float)) and o.get("order_total") <= 0]
    if bad_total:
        findings.append(_finding(
            "orders", "non_positive_order_total", "warning",
            len(bad_total),
            "order_total is zero or negative -- plausible refund/adjustment artifacts, but should be "
            "documented and excluded from average order value calculations.",
            bad_total,
        ))

    # -- invalid/missing required fields --
    missing_fields_rows = []
    for o in orders:
        missing = [f for f in REQUIRED_ORDER_FIELDS if _is_blank(o.get(f))]
        if missing:
            missing_fields_rows.append({"order_id": o.get("order_id"), "missing_fields": missing})
    if missing_fields_rows:
        findings.append(_finding(
            "orders", "missing_required_fields", "error",
            len(missing_fields_rows),
            f"Order rows missing one or more required fields ({', '.join(REQUIRED_ORDER_FIELDS)}) -- "
            "cannot be reliably modeled or joined without these.",
            missing_fields_rows,
        ))

    return findings


# --------------------------------------------------------------------------
# 2. Courier events validation
# --------------------------------------------------------------------------

def validate_courier_events(courier_events, orders):
    findings = []

    # Lookup of real order statuses, for cross-checking expected event presence.
    order_status_by_id = {}
    for o in orders:
        oid = o.get("order_id")
        if oid is not None and oid not in order_status_by_id:
            order_status_by_id[oid] = o.get("status")

    # -- malformed/mismatched order_ref --
    malformed_events = []
    groups = defaultdict(list)  # extracted numeric id -> list of events (grouped for detection only)
    for e in courier_events:
        oid, well_formed = _extract_order_ref_id(e.get("order_ref"))
        if not well_formed:
            malformed_events.append(e)
        if oid is not None:
            groups[oid].append(e)

    if malformed_events:
        findings.append(_finding(
            "courier_events", "malformed_order_ref", "error",
            len(malformed_events),
            "order_ref does not match the canonical 'ORD-<order_id>' format -- a naive string join "
            "against Orders DB would silently drop these events.",
            malformed_events,
        ))

    # -- missing picked_up events (for completed orders only; failed-before-pickup orders
    #    legitimately have no picked_up event and are not flagged here) --
    missing_pickup = []
    for oid, events in groups.items():
        if order_status_by_id.get(oid) != "completed":
            continue
        types_present = {e.get("event_type") for e in events}
        if "assigned" in types_present and "picked_up" not in types_present:
            missing_pickup.append({"order_id": oid, "event_types_present": sorted(types_present)})
    if missing_pickup:
        findings.append(_finding(
            "courier_events", "missing_picked_up_event", "error",
            len(missing_pickup),
            "Completed orders with an 'assigned' event but no 'picked_up' event -- courier leg "
            "duration cannot be computed for these.",
            missing_pickup,
        ))

    # -- missing delivered events for completed orders --
    missing_delivered = []
    for oid, events in groups.items():
        if order_status_by_id.get(oid) != "completed":
            continue
        types_present = {e.get("event_type") for e in events}
        if "delivered" not in types_present:
            missing_delivered.append({"order_id": oid, "event_types_present": sorted(types_present)})
    if missing_delivered:
        findings.append(_finding(
            "courier_events", "missing_delivered_event", "error",
            len(missing_delivered),
            "Orders marked 'completed' in the Orders DB have no matching 'delivered' event in the "
            "courier feed -- total delivery time and on-time status cannot be computed for these.",
            missing_delivered,
        ))

    # -- delivered timestamp earlier than picked_up --
    delivered_before_pickup = []
    for oid, events in groups.items():
        by_type = {e.get("event_type"): e for e in events if e.get("event_type") in ("picked_up", "delivered")}
        if "picked_up" in by_type and "delivered" in by_type:
            picked_up_ts = _parse_iso(by_type["picked_up"].get("event_time"))
            delivered_ts = _parse_iso(by_type["delivered"].get("event_time"))
            if picked_up_ts and delivered_ts and delivered_ts < picked_up_ts:
                delivered_before_pickup.append({
                    "order_id": oid,
                    "picked_up_at": by_type["picked_up"].get("event_time"),
                    "delivered_at": by_type["delivered"].get("event_time"),
                })
    if delivered_before_pickup:
        findings.append(_finding(
            "courier_events", "delivered_before_picked_up", "error",
            len(delivered_before_pickup),
            "'delivered' event timestamp is earlier than the 'picked_up' event timestamp for the same "
            "order -- indicates a clock-sync issue in the courier feed; duration cannot be trusted as-is.",
            delivered_before_pickup,
        ))

    # -- invalid event ordering where detectable (general chronological check
    #    across whichever canonical event types are present for an order) --
    invalid_ordering = []
    for oid, events in groups.items():
        by_type = {e.get("event_type"): e for e in events}
        present_sequence = [t for t in CANONICAL_COURIER_ORDER if t in by_type]
        timestamps = [(t, _parse_iso(by_type[t].get("event_time"))) for t in present_sequence]
        timestamps = [(t, ts) for t, ts in timestamps if ts is not None]
        for (t1, ts1), (t2, ts2) in zip(timestamps, timestamps[1:]):
            if ts1 > ts2:
                invalid_ordering.append({"order_id": oid, "out_of_order_pair": f"{t1} ({ts1}) after {t2} ({ts2})"})
                break
    if invalid_ordering:
        findings.append(_finding(
            "courier_events", "invalid_event_ordering", "warning",
            len(invalid_ordering),
            "Courier event sequence is not chronologically consistent with the expected "
            "assigned -> picked_up -> en_route -> delivered order.",
            invalid_ordering,
        ))

    return findings


# --------------------------------------------------------------------------
# 3. Restaurant POS validation
# --------------------------------------------------------------------------

def validate_restaurant_pos(restaurant_pos):
    findings = []

    # -- schema drift between files --
    schema_by_file = {}
    for row in restaurant_pos:
        fname = row.get("_source_file", "unknown")
        if fname not in schema_by_file:
            schema_by_file[fname] = frozenset(k for k in row.keys() if k != "_source_file")

    if schema_by_file:
        schema_counts = Counter(schema_by_file.values())
        baseline_schema = schema_counts.most_common(1)[0][0]
        drifted_files = {f: s for f, s in schema_by_file.items() if s != baseline_schema}
        if drifted_files:
            affected_rows = sum(1 for row in restaurant_pos if row.get("_source_file") in drifted_files)
            findings.append(_finding(
                "restaurant_pos", "schema_drift", "warning",
                affected_rows,
                f"{len(drifted_files)} restaurant POS file(s) use a different column layout than the "
                f"majority format. Baseline columns: {sorted(baseline_schema)}. "
                f"Drifted files/columns: {[(f, sorted(s)) for f, s in drifted_files.items()]}",
                [{"file": f, "columns": sorted(s)} for f, s in list(drifted_files.items())[:MAX_EXAMPLES]],
            ))

    # -- missing expected fields (no recognizable order-id column at all) --
    missing_field_rows = [row for row in restaurant_pos if _restaurant_row_order_id_field(row) is None]
    if missing_field_rows:
        findings.append(_finding(
            "restaurant_pos", "missing_order_id_field", "error",
            len(missing_field_rows),
            "Row has neither 'order_id' nor 'OrderID' column populated -- cannot be joined to an order at all.",
            missing_field_rows,
        ))

    # -- missing accepted/ready timestamps --
    missing_ts_rows = []
    for row in restaurant_pos:
        accepted_raw, ready_raw = _restaurant_row_timestamps(row)
        if _is_blank(accepted_raw) or _is_blank(ready_raw):
            missing_ts_rows.append({
                "order_id": _restaurant_row_order_id_field(row),
                "source_file": row.get("_source_file"),
                "accepted_time": accepted_raw,
                "ready_time": ready_raw,
            })
    if missing_ts_rows:
        findings.append(_finding(
            "restaurant_pos", "missing_prep_timestamp", "warning",
            len(missing_ts_rows),
            "accepted_time or food_ready_time is blank -- kitchen prep duration cannot be computed for these rows.",
            missing_ts_rows,
        ))

    # -- inconsistent order_id formats --
    prefixed_ids = []
    for row in restaurant_pos:
        oid = _restaurant_row_order_id_field(row)
        if oid and not oid.isdigit():
            prefixed_ids.append({"order_id": oid, "source_file": row.get("_source_file")})
    if prefixed_ids:
        findings.append(_finding(
            "restaurant_pos", "inconsistent_order_id_format", "warning",
            len(prefixed_ids),
            "Some restaurant files use a prefixed order_id convention (e.g. 'FE1000') instead of a "
            "plain numeric id -- must be normalized before joining to Orders DB / courier events.",
            prefixed_ids,
        ))

    # -- malformed timestamps (non-blank but unparseable as HH:MM:SS) --
    malformed_ts_rows = []
    for row in restaurant_pos:
        accepted_raw, ready_raw = _restaurant_row_timestamps(row)
        for label, raw in (("accepted_time", accepted_raw), ("food_ready_time", ready_raw)):
            if not _is_blank(raw) and _parse_clock_time(raw) is None:
                malformed_ts_rows.append({
                    "order_id": _restaurant_row_order_id_field(row),
                    "source_file": row.get("_source_file"),
                    "field": label,
                    "value": raw,
                })
    if malformed_ts_rows:
        findings.append(_finding(
            "restaurant_pos", "malformed_timestamp", "error",
            len(malformed_ts_rows),
            "Timestamp value present but not parseable as HH:MM:SS.",
            malformed_ts_rows,
        ))

    return findings


# --------------------------------------------------------------------------
# 4. Support tickets validation
# --------------------------------------------------------------------------

REQUIRED_TICKET_FIELDS = ["ticket_id", "category", "created_at", "resolution_status"]


def validate_support_tickets(tickets, orders):
    findings = []

    real_order_ids = {o.get("order_id") for o in orders if o.get("order_id") is not None}

    # -- null order_id --
    null_order_id = [t for t in tickets if t.get("order_id") is None]
    if null_order_id:
        findings.append(_finding(
            "support_tickets", "null_order_id", "warning",
            len(null_order_id),
            "Ticket has no order_id at all -- cannot be linked to any order; retain for overall "
            "ticket-volume reporting but exclude from order-linked metrics.",
            null_order_id,
        ))

    # -- orphaned order_id --
    orphaned = []
    for t in tickets:
        oid_raw = t.get("order_id")
        if oid_raw is None:
            continue
        try:
            oid = int(oid_raw)
        except (TypeError, ValueError):
            orphaned.append(t)
            continue
        if oid not in real_order_ids:
            orphaned.append(t)
    if orphaned:
        findings.append(_finding(
            "support_tickets", "orphaned_order_id", "warning",
            len(orphaned),
            "Ticket's order_id does not match any order in the Orders DB (typo, pre-migration ticket, "
            "or non-numeric id) -- document as a known linkage gap.",
            orphaned,
        ))

    # -- invalid/missing required ticket fields --
    missing_fields_rows = []
    for t in tickets:
        missing = [f for f in REQUIRED_TICKET_FIELDS if _is_blank(t.get(f))]
        if t.get("category") is not None and t.get("category") not in VALID_TICKET_CATEGORIES:
            missing.append("category (unrecognized value)")
        if missing:
            missing_fields_rows.append({"ticket_id": t.get("ticket_id"), "issues": missing})
    if missing_fields_rows:
        findings.append(_finding(
            "support_tickets", "missing_or_invalid_required_fields", "error",
            len(missing_fields_rows),
            f"Ticket rows missing/invalid required fields ({', '.join(REQUIRED_TICKET_FIELDS)}).",
            missing_fields_rows,
        ))

    return findings


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def validate_all(raw_data):
    """
    Runs every source-specific validator against the raw ingestion output
    and returns a structured report:

        {
            "findings": [ ...finding dicts... ],
            "summary": {
                "total_errors": int,
                "total_warnings": int,
                "by_source": {source: {"errors": int, "warnings": int}},
            },
        }
    """
    orders = raw_data.get("orders", [])
    courier_events = raw_data.get("courier_events", [])
    restaurant_pos = raw_data.get("restaurant_pos", [])
    support_tickets = raw_data.get("support_tickets", [])

    findings = []
    findings.extend(validate_orders(orders))
    findings.extend(validate_courier_events(courier_events, orders))
    findings.extend(validate_restaurant_pos(restaurant_pos))
    findings.extend(validate_support_tickets(support_tickets, orders))

    by_source = defaultdict(lambda: {"errors": 0, "warnings": 0})
    total_errors = 0
    total_warnings = 0
    for f in findings:
        key = "errors" if f["severity"] == "error" else "warnings"
        by_source[f["source"]][key] += f["affected_count"]
        if f["severity"] == "error":
            total_errors += f["affected_count"]
        else:
            total_warnings += f["affected_count"]

    summary = {
        "total_errors": total_errors,
        "total_warnings": total_warnings,
        "by_source": dict(by_source),
    }

    logger.info(f"Validation complete: {len(findings)} rule finding(s), "
                f"{total_errors} error-severity record(s), {total_warnings} warning-severity record(s).")
    for f in findings:
        logger.info(f"  [{f['severity'].upper()}] {f['source']}.{f['rule']}: {f['affected_count']} affected")

    return {"findings": findings, "summary": summary}


# --------------------------------------------------------------------------
# Standalone smoke test
# --------------------------------------------------------------------------

def main():
    import os
    import sys

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from src.ingestion.ingest import ingest_all, IngestionError

    try:
        raw_data = ingest_all()
    except IngestionError as e:
        logger.error(f"Ingestion failed, cannot run validation smoke test: {e}")
        return

    report = validate_all(raw_data)

    print("\n" + "=" * 60)
    print("VALIDATION REPORT")
    print("=" * 60)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()