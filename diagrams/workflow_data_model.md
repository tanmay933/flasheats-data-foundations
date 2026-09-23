# FlashEats Workflow and Data Model

## End-to-End Pipeline

```text
                    RAW CLIENT SYSTEMS
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
   Orders DB         Courier API       Restaurant POS
      SQL              API/JSON             CSV
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
                           ▼
                    Support Tickets
                         JSON
                           │
                           ▼
                    ┌─────────────┐
                    │  INGESTION  │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │ VALIDATION  │
                    └──────┬──────┘
                           │
              Quality findings preserved
                           │
                           ▼
                   ┌──────────────┐
                   │TRANSFORMATION│
                   └──────┬───────┘
                          │
              ID normalization
              Duplicate handling
              Timestamp normalization
              Cross-source joins
              Duration calculation
                          │
                          ▼
                   ORDER-LEVEL MODEL
                          │
                          ▼
                    ┌─────────────┐
                    │   METRICS   │
                    └──────┬──────┘
                           │
                           ▼
                  metrics_report.json
```

## Order Lifecycle

```text
Placed
  │
  ▼
Accepted
  │
  ▼
Preparing
  │
  ▼
Ready
  │
  ▼
Courier Assigned
  │
  ▼
Picked Up
  │
  ▼
En Route
  │
  ▼
Delivered
```

Possible terminal branches:

```text
Order
 ├── Delivered
 ├── Cancelled
 └── Failed
```

## Order-Level Data Model

```text
ORDER
├── order_id
├── restaurant_id
├── customer_id
├── order_status
├── order_total
├── placed_at
├── promised_by
├── accepted_at
├── food_ready_at
├── assigned_at
├── picked_up_at
├── delivered_at
├── assignment_lag_minutes
├── kitchen_time_minutes
├── courier_time_minutes
├── total_delivery_time_minutes
├── delay_minutes
├── delivery_outcome
├── delay_attribution
└── late_delivery_ticket_count
```

## Metric Relationships

```text
Order Lifecycle
      │
      ├── promised_by + delivered_at
      │        └── On-time Delivery Rate
      │
      ├── placed_at + delivered_at
      │        └── Average Delivery Time
      │
      ├── assigned_at + picked_up_at
      │        └── Assignment Lag
      │
      ├── accepted_at + food_ready_at
      │        └── Kitchen Time
      │
      ├── picked_up_at + delivered_at
      │        └── Courier Time
      │
      ├── order_status
      │        └── Failure / Cancellation Rate
      │
      └── support tickets
               └── Late-Delivery Complaint Rate
```

## Delay Attribution

The transformation applies explicit thresholds:

| Leg | Threshold |
|---|---|
| Kitchen time | > 20 min |
| Courier time | > 22 min |
| Assignment lag | > 8 min |

A single threshold breach is attributed to that leg.

Zero or multiple threshold breaches are classified as:

```text
mixed/unclear
```

This is a threshold-based operational classification, not causal analysis.