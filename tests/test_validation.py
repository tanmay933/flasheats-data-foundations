import unittest

from src.validation.validate import (
    validate_orders,
    validate_courier_events,
    validate_restaurant_pos,
    validate_support_tickets,
)


class TestOrderValidation(unittest.TestCase):

    def test_duplicate_order_id(self):
        orders = [
            {
                "order_id": "1001",
                "order_total": 500,
                "order_status": "completed",
            },
            {
                "order_id": "1001",
                "order_total": 500,
                "order_status": "completed",
            },
        ]

        findings = validate_orders(orders)

        duplicate_findings = [
            f for f in findings
            if f["rule"] == "duplicate_order_id"
        ]

        self.assertEqual(len(duplicate_findings), 1)
        self.assertEqual(duplicate_findings[0]["affected_count"], 1)

    def test_negative_order_total(self):
        orders = [
            {
                "order_id": "1001",
                "order_total": -100,
                "order_status": "completed",
            }
        ]

        findings = validate_orders(orders)

        negative_findings = [
            f for f in findings
            if f["rule"] == "non_positive_order_total"
        ]

        self.assertEqual(len(negative_findings), 1)
        self.assertEqual(negative_findings[0]["affected_count"], 1)


class TestCourierValidation(unittest.TestCase):

    def test_malformed_order_reference(self):
        courier_events = [
            {
                "order_ref": "INVALID",
                "event": "assigned",
                "timestamp": "2026-08-01T10:00:00",
            }
        ]

        orders = [
            {
                "order_id": "1001",
                "order_status": "completed",
            }
        ]

        findings = validate_courier_events(courier_events, orders)

        malformed_findings = [
            f for f in findings
            if f["rule"] == "malformed_order_ref"
        ]

        self.assertEqual(len(malformed_findings), 1)
        self.assertEqual(malformed_findings[0]["affected_count"], 1)


class TestRestaurantValidation(unittest.TestCase):

    def test_missing_prep_timestamp(self):
        restaurant_rows = [
            {
                "order_id": "1001",
                "restaurant_id": "r01",
                "accepted_at": None,
                "food_ready_at": "12:30:00",
            }
        ]

        findings = validate_restaurant_pos(restaurant_rows)

        missing_findings = [
            f for f in findings
            if f["rule"] == "missing_prep_timestamp"
        ]

        self.assertEqual(len(missing_findings), 1)
        self.assertEqual(missing_findings[0]["affected_count"], 1)


class TestSupportTicketValidation(unittest.TestCase):

    def test_null_ticket_order_id(self):
        support_tickets = [
            {
                "ticket_id": "T001",
                "order_id": None,
                "reason": "late_delivery",
            }
        ]

        orders = [
            {
                "order_id": "1001",
            }
        ]

        findings = validate_support_tickets(
            support_tickets,
            orders
        )

        null_findings = [
            f for f in findings
            if f["rule"] == "null_order_id"
        ]

        self.assertEqual(len(null_findings), 1)
        self.assertEqual(null_findings[0]["affected_count"], 1)


if __name__ == "__main__":
    unittest.main()