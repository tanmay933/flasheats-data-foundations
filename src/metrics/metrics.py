"""
src/metrics/metrics.py

Metrics layer for the FlashEats pipeline.

Calculates the five business KPIs for the delivery-reliability question
("How reliable is FlashEats delivery, and where are delays coming from?")
from the analysis-ready order-level dataset produced by
src/transformation/transform.py.

This module does not re-derive or fix anything: it consumes the fields
transform.py already computed (delivery_outcome, delay_attribution, the
duration fields, ticket counts) and aggregates them. Any record whose
relevant field is None is excluded from that metric's calculation and
counted explicitly in the metric's "exclusions" -- never silently dropped.

Each metric result includes:
    value, unit, numerator/denominator (where applicable),
    valid_record_count, exclusions, and a knowledge_status block
    distinguishing Known / Unknown / Assumption / Limitation.
"""

import json
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("metrics")

DELAY_ATTRIBUTION_CATEGORIES = ["kitchen_delay", "courier_delay", "assignment_delay", "mixed/unclear"]


def _pct(numerator, denominator):
    if not denominator:
        return None
    return round(100 * numerator / denominator, 2)


def _avg(values):
    if not values:
        return None
    return round(sum(values) / len(values), 2)


# --------------------------------------------------------------------------
# Metric 1: On-time delivery rate
# --------------------------------------------------------------------------

def calc_on_time_delivery_rate(orders):
    usable = [o for o in orders if o["delivery_outcome"] in ("on_time", "late")]
    on_time = [o for o in usable if o["delivery_outcome"] == "on_time"]
    unavailable_completed = [
        o for o in orders if o["order_status"] == "completed" and o["delivery_outcome"] == "unavailable"
    ]
    non_completed = [o for o in orders if o["order_status"] in ("cancelled", "failed")]

    numerator = len(on_time)
    denominator = len(usable)

    return {
        "value": _pct(numerator, denominator),
        "unit": "percent",
        "numerator": numerator,
        "denominator": denominator,
        "valid_record_count": denominator,
        "exclusions": {
            "completed_orders_excluded_unavailable_outcome": len(unavailable_completed),
            "non_completed_orders_excluded": len(non_completed),
        },
        "knowledge_status": {
            "known": "Directly calculated from delivery_outcome on completed orders with a usable "
                     "on_time/late classification.",
            "unknown": f"{len(unavailable_completed)} completed order(s) have no usable delivery_outcome "
                       "(missing/invalid delivered_at or promised_by) and are excluded from both "
                       "numerator and denominator, not counted as on-time or late.",
            "assumption": "Inherits transform.py's rule that delivery_outcome is only 'on_time'/'late' "
                          "when both delivered_at and promised_by are present and valid.",
            "limitation": "Cancelled and failed orders are excluded from this rate entirely; see the "
                          "failure/cancellation rate metric for those.",
        },
    }


# --------------------------------------------------------------------------
# Metric 2: Average end-to-end delivery time (+ component legs)
# --------------------------------------------------------------------------

def _avg_component(orders, field_name):
    completed = [o for o in orders if o["order_status"] == "completed"]
    valid_values = [o[field_name] for o in completed if o[field_name] is not None]
    missing_count = len(completed) - len(valid_values)
    return {
        "value": _avg(valid_values),
        "unit": "minutes",
        "valid_record_count": len(valid_values),
        "exclusions": {"completed_orders_missing_this_value": missing_count},
    }


def calc_average_delivery_time(orders):
    total = _avg_component(orders, "total_delivery_time_minutes")
    kitchen = _avg_component(orders, "kitchen_time_minutes")
    courier = _avg_component(orders, "courier_time_minutes")
    assignment = _avg_component(orders, "assignment_lag_minutes")

    return {
        "value": total["value"],
        "unit": "minutes",
        "valid_record_count": total["valid_record_count"],
        "exclusions": total["exclusions"],
        "components": {
            "average_kitchen_time_minutes": kitchen,
            "average_courier_time_minutes": courier,
            "average_assignment_lag_minutes": assignment,
        },
        "knowledge_status": {
            "known": "Directly averaged from total_delivery_time_minutes / kitchen_time_minutes / "
                     "courier_time_minutes / assignment_lag_minutes on completed orders with a valid value.",
            "unknown": "Orders missing the relevant source timestamp(s) (e.g. no restaurant POS row, "
                       "no delivered event) contribute no value to that specific average; counts are "
                       "reported per component above.",
            "assumption": "Restaurant POS timestamps were treated as IST and converted to UTC before "
                          "these durations were computed (see transform.py assumptions).",
            "limitation": "Each component average is computed only over the orders where that specific "
                          "leg's data was available -- the four averages are not necessarily computed "
                          "over the same set of orders.",
        },
    }


# --------------------------------------------------------------------------
# Metric 3: Delay attribution
# --------------------------------------------------------------------------

def calc_delay_attribution(orders):
    late_orders = [o for o in orders if o["delivery_outcome"] == "late"]
    usable = [o for o in late_orders if o["delay_attribution"] in DELAY_ATTRIBUTION_CATEGORIES]
    unusable = [o for o in late_orders if o["delay_attribution"] not in DELAY_ATTRIBUTION_CATEGORIES]

    denominator = len(usable)
    breakdown = {}
    for category in DELAY_ATTRIBUTION_CATEGORIES:
        count = sum(1 for o in usable if o["delay_attribution"] == category)
        breakdown[category] = {
            "count": count,
            "percentage": _pct(count, denominator),
            "unit": "percent",
        }

    return {
        "denominator": denominator,
        "valid_record_count": denominator,
        "breakdown": breakdown,
        "exclusions": {
            "late_orders_without_usable_attribution": len(unusable),
        },
        "knowledge_status": {
            "known": "Directly tallied from transform.py's delay_attribution field on orders with "
                     "delivery_outcome == 'late'.",
            "unknown": f"{len(unusable)} late order(s) had no computable delay_attribution (missing "
                       "component durations) and are excluded from this breakdown.",
            "assumption": "Uses transform.py's threshold rule: a leg is named as the cause only if its "
                          "duration exceeds its own normal baseline; zero or multiple legs exceeding "
                          "baseline is reported as 'mixed/unclear'.",
            "limitation": "This is a simple threshold-based attribution, not a statistical or causal "
                          "analysis -- it does not account for legs with partially missing data or "
                          "interacting delays.",
        },
    }


# --------------------------------------------------------------------------
# Metric 4: Order failure/cancellation rate
# --------------------------------------------------------------------------

def calc_failure_cancellation_rate(orders):
    cancelled = [o for o in orders if o["order_status"] == "cancelled"]
    failed = [o for o in orders if o["order_status"] == "failed"]
    denominator = len(orders)
    numerator = len(cancelled) + len(failed)

    return {
        "value": _pct(numerator, denominator),
        "unit": "percent",
        "numerator": numerator,
        "cancelled_count": len(cancelled),
        "failed_count": len(failed),
        "denominator": denominator,
        "valid_record_count": denominator,
        "exclusions": {},
        "knowledge_status": {
            "known": "Directly calculated from order_status across all unique (deduplicated) orders in "
                     "the transformed dataset.",
            "unknown": "None -- order_status is a required field and present for every order.",
            "assumption": "Denominator is the deduplicated order count from transform.py (exact "
                          "duplicate Orders DB rows already removed).",
            "limitation": "Does not distinguish cancellation reasons (before vs after restaurant "
                          "acceptance) or failure stage (before vs after pickup) -- both are collapsed "
                          "into a single failure/cancellation rate here.",
        },
    }


# --------------------------------------------------------------------------
# Metric 5: Late-delivery complaint rate
# --------------------------------------------------------------------------

def calc_late_delivery_complaint_rate(orders):
    completed = [o for o in orders if o["order_status"] == "completed"]
    late = [o for o in orders if o["delivery_outcome"] == "late"]

    linked_late_tickets = sum(o["late_delivery_ticket_count"] for o in completed)
    completed_with_ticket = [o for o in completed if o["late_delivery_ticket_count"] > 0]
    late_with_ticket = [o for o in late if o["late_delivery_ticket_count"] > 0]

    denominator = len(completed)

    return {
        "value": _pct(linked_late_tickets, denominator),
        "unit": "percent (linked late_delivery tickets per completed order)",
        "numerator": linked_late_tickets,
        "denominator": denominator,
        "valid_record_count": denominator,
        "completed_orders_with_late_delivery_ticket": len(completed_with_ticket),
        "late_orders_with_late_delivery_ticket": len(late_with_ticket),
        "exclusions": {
            "note": "Only tickets successfully linked to a known order_id are counted (see "
                    "transform.py metadata for unlinked_tickets); null/orphaned tickets are excluded.",
        },
        "knowledge_status": {
            "known": "Directly counted from late_delivery_ticket_count on completed orders, and cross-"
                     "tabulated against delivery_outcome == 'late'.",
            "unknown": "Ticket counts for orders not present in the Orders DB (orphaned/null order_id "
                       "tickets) are not reflected here; see transformation metadata for their totals.",
            "assumption": "A ticket is only linked if its order_id parsed as an integer and matched a "
                          "real, known order_id (transform.py rule).",
            "limitation": "A late_delivery ticket does not prove the delivery was actually late (and "
                          "vice versa) -- this metric reports the observed relationship in the data "
                          "only, not a validated causal or ground-truth link.",
        },
    }


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def calculate_metrics(transform_result):
    """
    Args:
        transform_result: the dict returned by src/transformation/transform.py's
            transform_all(), i.e. {"orders": [...], "metadata": {...}}.

    Returns:
        {
            "metrics": {
                "on_time_delivery_rate": {...},
                "average_delivery_time": {...},
                "delay_attribution": {...},
                "failure_cancellation_rate": {...},
                "late_delivery_complaint_rate": {...},
            },
            "data_quality_context": {...},
            "kpi_summary": {...},
        }
    """
    orders = transform_result.get("orders", [])
    transform_metadata = transform_result.get("metadata", {})

    metrics = {
        "on_time_delivery_rate": calc_on_time_delivery_rate(orders),
        "average_delivery_time": calc_average_delivery_time(orders),
        "delay_attribution": calc_delay_attribution(orders),
        "failure_cancellation_rate": calc_failure_cancellation_rate(orders),
        "late_delivery_complaint_rate": calc_late_delivery_complaint_rate(orders),
    }

    data_quality_context = {
        "total_orders_in_scope": len(orders),
        "duplicates_removed_upstream": transform_metadata.get("duplicates_removed"),
        "unlinked_courier_events": transform_metadata.get("unlinked_courier_events"),
        "unlinked_restaurant_rows": transform_metadata.get("unlinked_restaurant_rows"),
        "unlinked_tickets": transform_metadata.get("unlinked_tickets"),
        "assumptions_inherited_from_transformation": transform_metadata.get("assumptions", []),
        "known_data_quality_issues_from_validation": transform_metadata.get("known_data_quality_issues"),
    }

    kpi_summary = {
        "business_question": "How reliable is FlashEats delivery, and where are delays coming from?",
        "on_time_delivery_rate_pct": metrics["on_time_delivery_rate"]["value"],
        "on_time_delivery_rate_basis": (
            f"{metrics['on_time_delivery_rate']['numerator']} / "
            f"{metrics['on_time_delivery_rate']['denominator']} completed orders with a usable outcome"
        ),
        "average_total_delivery_time_minutes": metrics["average_delivery_time"]["value"],
        "top_delay_cause": max(
            metrics["delay_attribution"]["breakdown"].items(),
            key=lambda kv: kv[1]["count"],
        )[0] if metrics["delay_attribution"]["denominator"] else None,
        "failure_cancellation_rate_pct": metrics["failure_cancellation_rate"]["value"],
        "late_delivery_complaint_rate_pct": metrics["late_delivery_complaint_rate"]["value"],
    }

    logger.info("Metrics calculated:")
    logger.info(f"  On-time delivery rate: {metrics['on_time_delivery_rate']['value']}% "
                f"({metrics['on_time_delivery_rate']['numerator']}/{metrics['on_time_delivery_rate']['denominator']})")
    logger.info(f"  Avg total delivery time: {metrics['average_delivery_time']['value']} min "
                f"(n={metrics['average_delivery_time']['valid_record_count']})")
    logger.info(f"  Delay attribution denominator: {metrics['delay_attribution']['denominator']} late orders")
    logger.info(f"  Failure/cancellation rate: {metrics['failure_cancellation_rate']['value']}%")
    logger.info(f"  Late-delivery complaint rate: {metrics['late_delivery_complaint_rate']['value']}%")

    return {
        "metrics": metrics,
        "data_quality_context": data_quality_context,
        "kpi_summary": kpi_summary,
    }


# --------------------------------------------------------------------------
# Standalone smoke test
# --------------------------------------------------------------------------

def main():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from src.ingestion.ingest import ingest_all, IngestionError
    from src.validation.validate import validate_all
    from src.transformation.transform import transform_all

    try:
        raw_data = ingest_all()
    except IngestionError as e:
        logger.error(f"Ingestion failed, cannot run metrics smoke test: {e}")
        return

    validation_report = validate_all(raw_data)
    transform_result = transform_all(raw_data, validation_report=validation_report)
    report = calculate_metrics(transform_result)

    print("\n" + "=" * 60)
    print("FLASHEATS DELIVERY RELIABILITY -- KPI SUMMARY")
    print("=" * 60)
    summary = report["kpi_summary"]
    print(f"Business question: {summary['business_question']}")
    print(f"On-time delivery rate:        {summary['on_time_delivery_rate_pct']}%  "
          f"({summary['on_time_delivery_rate_basis']})")
    print(f"Average total delivery time:  {summary['average_total_delivery_time_minutes']} min")
    print(f"Top delay cause:              {summary['top_delay_cause']}")
    print(f"Failure/cancellation rate:    {summary['failure_cancellation_rate_pct']}%")
    print(f"Late-delivery complaint rate: {summary['late_delivery_complaint_rate_pct']}%")

    print("\nFull metrics report:")
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()