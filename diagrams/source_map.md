# FlashEats Source Map

## Business Question

**How reliable is FlashEats delivery, and where are delays coming from?**

## Source Systems

| Source System | Retrieval Mode | Grain | Key Information | Main Owner / System Role | Known Gaps |
|---|---|---|---|---|---|
| Orders DB | SQL / SQLite | One row per order | order ID, restaurant, customer, status, promised delivery time, order total | Order management system | Duplicate order rows, null/non-positive totals |
| Courier Tracking | API / mock API adapter | One row per courier event | assignment, pickup, delivery events and timestamps | Courier tracking system | Missing events, malformed order references, invalid event ordering |
| Restaurant POS | CSV files | One row per restaurant/order preparation record | accepted and food-ready timestamps | Restaurant POS systems | Schema drift, inconsistent IDs, missing preparation timestamps |
| Support Tickets | JSON files | One row per support ticket | ticket reason, order reference, ticket timestamp | Customer support system | Null and orphaned order IDs |

## Cross-System Join Key

The canonical business identifier is:

```text
order_id
```

Different systems represent the same order differently:

| System | Representation |
|---|---|
| Orders DB | `1000` |
| Courier API | `ORD-1000` |
| Restaurant POS | `1000` or `FE1000` |
| Support Tickets | `1000` |

The transformation layer normalizes these representations before joining.

## Retrieval Completeness

The pipeline retrieves:

- 408 order rows from SQL
- 1,404 courier events through the API adapter
- 384 restaurant POS rows across 158 CSV files
- 65 support tickets from JSON

Raw inputs remain unchanged. Validation records quality issues before transformation.

## Important Assumptions

- Orders and courier timestamps are treated as UTC.
- Restaurant POS timestamps are assumed to be IST because the source files contain local times without timezone information.
- The restaurant filename supplies the date for its timestamp.
- Malformed courier references are not force-matched.
- Null or invalid duration endpoints are left unavailable rather than guessed.