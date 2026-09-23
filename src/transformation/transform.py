"""
src/transformation/transform.py

Transformation layer for the FlashEats pipeline.

Converts the raw, fragmented source data returned by src/ingestion/ingest.py
into a single clean, analysis-ready, order-level dataset. This module:

  - normalizes order_id representations across sources for joining
  - deduplicates exact duplicate Orders rows (documented, not hidden)
  - converts all timestamps into a consistent UTC-aware representation
  - computes workflow durations and delay attribution using an explicit,
    explainable rule
  - never fabricates data: any value that cannot be reliably derived from
    the raw records is left as None and reflected in the output metadata

KPI aggregation (business metrics, evidence table, etc.) is NOT done here --
that belongs in src/metrics/metrics.py. This module only builds the clean
order-level rows those metrics will be calculated from.
"""

import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("transformation")

UTC = timezone.utc
IST_OFFSET = timedelta(hours=5, minutes=30)

ORDER_REF_PATTERN = re.compile(r"^ORD-(\d+)$")
RESTAURANT_FILE_DATE_PATTERN = re.compile(r"_(\d{4}-\d{2}-\d{2})\.csv$")

# Baselines the delay-attribution rule compares against. These are the
# "normal" upper bounds used when the synthetic data was generated
# (see data/generate_data.py: prep/courier/assignment baseline ranges).
# A leg is considered the delay cause if its duration clearly exceeds its
# own normal range -- this is a simple, explainable rule, not a statistical
# inference, and it does not claim causality beyond what the timestamps show.
KITCHEN_BASELINE_MAX_MINUTES = 20
COURIER_BASELINE_MAX_MINUTES = 22
ASSIGNMENT_BASELINE_MAX_MINUTES = 8

ASSUMPTIONS = [
    "Restaurant POS timestamps (HH:MM:SS) carry no date or timezone; the order date is taken from the "
    "source filename (<restaurant>_<YYYY-MM-DD>.csv) and the time is assumed to be IST (UTC+5:30). "
    "All timestamps in the output are converted to UTC-aware ISO strings for consistent calculation.",
    "Duplicate Orders DB rows (exact re-insert, same order_id) are deduplicated by order_id, keeping "
    "the first occurrence encountered; the count removed is reported in metadata, not hidden.",
    "Courier order_ref values that do not match the canonical 'ORD-<order_id>' pattern are never "
    "force-matched to an order by heuristic; they are left unlinked and counted in metadata.",
    "Restaurant POS order_id values are normalized by parsing a plain integer or stripping a leading "
    "'FE' vendor prefix (drifted-schema restaurants); anything else is left unlinked and counted.",
    "Duration metrics (assignment/kitchen/courier/total) are only calculated when both endpoint "
    "timestamps are present and chronologically valid (end timestamp not before start timestamp); "
    "otherwise the metric is left null rather than guessed.",
    "delivery_outcome is 'on_time' or 'late' only when order_status is 'completed' and both "
    "delivered_at and promised_by are present and delay_minutes could be computed; orders missing "
    "that data are marked 'unavailable' rather than assumed on-time or late.",
    f"delay_attribution uses a simple threshold rule: a leg is named as the cause if its duration "
    f"exceeds its normal baseline (kitchen > {KITCHEN_BASELINE_MAX_MINUTES}m, "
    f"courier > {COURIER_BASELINE_MAX_MINUTES}m, assignment > {ASSIGNMENT_BASELINE_MAX_MINUTES}m); "
    f"zero or more than one leg exceeding its baseline is reported as 'mixed/unclear'.",
    "Support ticket counts on each order only include tickets whose order_id parses to a real, "
    "known order_id; null and orphaned tickets are excluded from order-level counts but their "
    "totals are preserved in transformation metadata.",
]


# --------------------------------------------------------------------------
# Timestamp helpers
# --------------------------------------------------------------------------

def _parse_utc_iso(ts):
    """Parses an ISO timestamp like '2026-08-04T12:31:00Z' into a UTC-aware datetime. None on failure/blank."""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.rstrip("Z"))
    except (ValueError, AttributeError):
        return None
    return dt.replace(tzinfo=UTC)


def _extract_date_from_filename(filename):
    if not filename:
        return None
    m = RESTAURANT_FILE_DATE_PATTERN.search(filename)
    return m.group(1) if m else None


def _restaurant_timestamp_to_utc(date_str, time_str):
    """
    Combines a filename-derived date with a bare HH:MM:SS restaurant POS time,
    treats it as IST, and converts to a UTC-aware datetime. None if either
    part is missing or unparseable.
    """
    if not date_str or not time_str:
        return None
    try:
        naive_dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    utc_dt = naive_dt - IST_OFFSET
    return utc_dt.replace(tzinfo=UTC)


def _duration_minutes(start, end):
    """Minutes between two UTC-aware datetimes. None if either is missing or end precedes start."""
    if start is None or end is None:
        return None
    if end < start:
        return None
    return round((end - start).total_seconds() / 60, 2)


def _iso_or_none(dt):
    return dt.isoformat() if dt is not None else None


# --------------------------------------------------------------------------
# ID normalization helpers (detection/joining only -- raw records untouched)
# --------------------------------------------------------------------------

def _canonical_id_from_courier_ref(order_ref):
    """Returns int order_id if order_ref matches 'ORD-<digits>' exactly, else None (never guessed)."""
    if not order_ref:
        return None
    m = ORDER_REF_PATTERN.match(order_ref)
    return int(m.group(1)) if m else None


def _canonical_id_from_restaurant_row(oid_raw):
    """Returns int order_id from a plain numeric id or an 'FE<digits>' prefixed id, else None."""
    if not oid_raw:
        return None
    oid_raw = oid_raw.strip()
    if oid_raw.isdigit():
        return int(oid_raw)
    if oid_raw.startswith("FE") and oid_raw[2:].isdigit():
        return int(oid_raw[2:])
    return None


def _restaurant_row_order_id_raw(row):
    return row.get("order_id") if "order_id" in row else row.get("OrderID")


def _restaurant_row_times_raw(row):
    if "accepted_time" in row or "food_ready_time" in row:
        return row.get("accepted_time"), row.get("food_ready_time")
    return row.get("Accepted_At"), row.get("Ready_At")


# --------------------------------------------------------------------------
# Grouping / deduplication
# --------------------------------------------------------------------------

def _dedup_orders(orders):
    """Keeps the first occurrence of each order_id. Returns (deduped_list, duplicates_removed_count)."""
    seen = set()
    deduped = []
    duplicates_removed = 0
    for o in orders:
        oid = o.get("order_id")
        if oid in seen:
            duplicates_removed += 1
            continue
        seen.add(oid)
        deduped.append(o)
    return deduped, duplicates_removed


def _group_courier_events(courier_events):
    """Groups events by canonical order_id. Malformed order_ref values are returned separately, unlinked."""
    grouped = defaultdict(dict)  # canonical_id -> {event_type: event}
    unlinked = []
    for e in courier_events:
        cid = _canonical_id_from_courier_ref(e.get("order_ref"))
        if cid is None:
            unlinked.append(e)
            continue
        event_type = e.get("event_type")
        if event_type not in grouped[cid]:  # keep first occurrence per type if ever duplicated
            grouped[cid][event_type] = e
    return grouped, unlinked


def _group_restaurant_rows(restaurant_pos):
    """Groups rows by canonical order_id (1:1 by construction). Unmappable rows returned separately."""
    grouped = {}
    unlinked = []
    for row in restaurant_pos:
        oid_raw = _restaurant_row_order_id_raw(row)
        cid = _canonical_id_from_restaurant_row(oid_raw)
        if cid is None:
            unlinked.append(row)
            continue
        grouped.setdefault(cid, row)
    return grouped, unlinked


def _group_tickets(tickets, real_order_ids):
    """Groups tickets by canonical order_id, only when it matches a real known order. Others returned unlinked."""
    grouped = defaultdict(list)
    unlinked = []
    for t in tickets:
        oid_raw = t.get("order_id")
        if oid_raw is None:
            unlinked.append(t)
            continue
        try:
            oid = int(oid_raw)
        except (TypeError, ValueError):
            unlinked.append(t)
            continue
        if oid not in real_order_ids:
            unlinked.append(t)
            continue
        grouped[oid].append(t)
    return grouped, unlinked


# --------------------------------------------------------------------------
# Delay attribution (simple, explainable rule -- see ASSUMPTIONS above)
# --------------------------------------------------------------------------

def _attribute_delay(kitchen_time_minutes, courier_time_minutes, assignment_lag_minutes):
    exceeded = []
    if kitchen_time_minutes is not None and kitchen_time_minutes > KITCHEN_BASELINE_MAX_MINUTES:
        exceeded.append("kitchen_delay")
    if courier_time_minutes is not None and courier_time_minutes > COURIER_BASELINE_MAX_MINUTES:
        exceeded.append("courier_delay")
    if assignment_lag_minutes is not None and assignment_lag_minutes > ASSIGNMENT_BASELINE_MAX_MINUTES:
        exceeded.append("assignment_delay")

    if len(exceeded) == 1:
        return exceeded[0]
    return "mixed/unclear"  # zero or multiple legs exceeded baseline -- not a single clean cause


# --------------------------------------------------------------------------
# Per-order record construction
# --------------------------------------------------------------------------

def _build_order_record(order, courier_grouped, restaurant_grouped, tickets_grouped):
    oid = order.get("order_id")
    order_status = order.get("status")

    order_placed_at = _parse_utc_iso(order.get("order_placed_at"))
    promised_by = _parse_utc_iso(order.get("promised_by"))

    events_by_type = courier_grouped.get(oid, {})
    courier_assigned_at = _parse_utc_iso(events_by_type["assigned"]["event_time"]) if "assigned" in events_by_type else None
    picked_up_at = _parse_utc_iso(events_by_type["picked_up"]["event_time"]) if "picked_up" in events_by_type else None
    delivered_at = _parse_utc_iso(events_by_type["delivered"]["event_time"]) if "delivered" in events_by_type else None

    restaurant_row = restaurant_grouped.get(oid)
    restaurant_accepted_at = None
    food_ready_at = None
    if restaurant_row is not None:
        date_str = _extract_date_from_filename(restaurant_row.get("_source_file"))
        accepted_raw, ready_raw = _restaurant_row_times_raw(restaurant_row)
        restaurant_accepted_at = _restaurant_timestamp_to_utc(date_str, accepted_raw)
        food_ready_at = _restaurant_timestamp_to_utc(date_str, ready_raw)

    assignment_lag_minutes = _duration_minutes(order_placed_at, courier_assigned_at)
    kitchen_time_minutes = _duration_minutes(restaurant_accepted_at, food_ready_at)
    courier_time_minutes = _duration_minutes(picked_up_at, delivered_at)
    total_delivery_time_minutes = _duration_minutes(order_placed_at, delivered_at)

    # delay_minutes is a signed difference, not a duration -- no start<=end constraint applies.
    delay_minutes = None
    if delivered_at is not None and promised_by is not None:
        delay_minutes = round((delivered_at - promised_by).total_seconds() / 60, 2)

    if order_status == "cancelled":
        delivery_outcome = "cancelled"
    elif order_status == "failed":
        delivery_outcome = "failed"
    elif order_status == "completed" and delay_minutes is not None:
        delivery_outcome = "late" if delay_minutes > 0 else "on_time"
    else:
        # completed in name, but missing/invalid delivered_at or promised_by --
        # never guessed as on_time or late.
        delivery_outcome = "unavailable"

    delay_attribution = None
    if delivery_outcome == "late":
        delay_attribution = _attribute_delay(kitchen_time_minutes, courier_time_minutes, assignment_lag_minutes)

    order_tickets = tickets_grouped.get(oid, [])
    support_ticket_count = len(order_tickets)
    late_delivery_ticket_count = sum(1 for t in order_tickets if t.get("category") == "late_delivery")

    return {
        "order_id": oid,
        "customer_id": order.get("customer_id"),
        "restaurant_id": order.get("restaurant_id"),
        "order_status": order_status,
        "order_total": order.get("order_total"),
        "order_placed_at": _iso_or_none(order_placed_at),
        "promised_by": _iso_or_none(promised_by),
        "courier_assigned_at": _iso_or_none(courier_assigned_at),
        "picked_up_at": _iso_or_none(picked_up_at),
        "delivered_at": _iso_or_none(delivered_at),
        "restaurant_accepted_at": _iso_or_none(restaurant_accepted_at),
        "food_ready_at": _iso_or_none(food_ready_at),
        "assignment_lag_minutes": assignment_lag_minutes,
        "kitchen_time_minutes": kitchen_time_minutes,
        "courier_time_minutes": courier_time_minutes,
        "total_delivery_time_minutes": total_delivery_time_minutes,
        "delay_minutes": delay_minutes,
        "delivery_outcome": delivery_outcome,
        "delay_attribution": delay_attribution,
        "support_ticket_count": support_ticket_count,
        "late_delivery_ticket_count": late_delivery_ticket_count,
    }


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def transform_all(raw_data, validation_report=None):
    """
    Builds the analysis-ready order-level dataset from raw ingestion output.

    Args:
        raw_data: the dict returned by src/ingestion/ingest.py's ingest_all().
        validation_report: optional dict returned by src/validation/validate.py's
            validate_all(). When provided, its summary is attached to the
            transformation metadata for traceability -- it does not change
            how transformation behaves; known issues are never hidden either way.

    Returns:
        {"orders": [...], "metadata": {...}}
    """
    orders_raw = raw_data.get("orders", [])
    courier_events_raw = raw_data.get("courier_events", [])
    restaurant_pos_raw = raw_data.get("restaurant_pos", [])
    tickets_raw = raw_data.get("support_tickets", [])

    input_counts = {
        "orders": len(orders_raw),
        "courier_events": len(courier_events_raw),
        "restaurant_pos": len(restaurant_pos_raw),
        "support_tickets": len(tickets_raw),
    }

    deduped_orders, duplicates_removed = _dedup_orders(orders_raw)
    real_order_ids = {o.get("order_id") for o in deduped_orders if o.get("order_id") is not None}

    courier_grouped, unlinked_courier = _group_courier_events(courier_events_raw)
    restaurant_grouped, unlinked_restaurant = _group_restaurant_rows(restaurant_pos_raw)
    tickets_grouped, unlinked_tickets = _group_tickets(tickets_raw, real_order_ids)

    output_orders = [
        _build_order_record(o, courier_grouped, restaurant_grouped, tickets_grouped)
        for o in deduped_orders
    ]

    metadata = {
        "input_counts": input_counts,
        "output_count": len(output_orders),
        "duplicates_removed": duplicates_removed,
        "unlinked_courier_events": len(unlinked_courier),
        "unlinked_restaurant_rows": len(unlinked_restaurant),
        "unlinked_tickets": len(unlinked_tickets),
        "assumptions": ASSUMPTIONS,
    }
    if validation_report is not None:
        metadata["known_data_quality_issues"] = validation_report.get("summary")

    logger.info(
        f"Transformation complete: {input_counts['orders']} input order rows -> "
        f"{len(output_orders)} order-level records "
        f"({duplicates_removed} duplicates removed, "
        f"{len(unlinked_courier)} unlinked courier events, "
        f"{len(unlinked_restaurant)} unlinked restaurant rows, "
        f"{len(unlinked_tickets)} unlinked tickets)"
    )

    return {"orders": output_orders, "metadata": metadata}


# --------------------------------------------------------------------------
# Standalone smoke test
# --------------------------------------------------------------------------

def main():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from src.ingestion.ingest import ingest_all, IngestionError
    from src.validation.validate import validate_all

    try:
        raw_data = ingest_all()
    except IngestionError as e:
        logger.error(f"Ingestion failed, cannot run transformation smoke test: {e}")
        return

    validation_report = validate_all(raw_data)
    result = transform_all(raw_data, validation_report=validation_report)

    print("\n" + "=" * 60)
    print("TRANSFORMATION SMOKE TEST")
    print("=" * 60)
    print(f"Input counts:  {result['metadata']['input_counts']}")
    print(f"Output count:  {result['metadata']['output_count']}")
    print(f"Duplicates removed: {result['metadata']['duplicates_removed']}")
    print(f"Unlinked courier events: {result['metadata']['unlinked_courier_events']}")
    print(f"Unlinked restaurant rows: {result['metadata']['unlinked_restaurant_rows']}")
    print(f"Unlinked tickets: {result['metadata']['unlinked_tickets']}")

    print("\nSample transformed orders:")
    for record in result["orders"][:3]:
        print(json.dumps(record, indent=2, default=str))

    print("\nAssumptions documented:")
    for a in result["metadata"]["assumptions"]:
        print(f"  - {a}")


if __name__ == "__main__":
    main()