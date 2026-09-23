"""
data/generate_data.py

Generates synthetic "raw" source data for the FlashEats FDE Data Foundations
assignment. Produces four fragmented sources that the later pipeline will
ingest, validate, and join on order_id:

    1. Orders DB       -> data/raw/orders/orders.db            (SQLite)
    2. Courier events  -> data/raw/courier/courier_events.json  (JSON)
    3. Restaurant POS  -> data/raw/restaurants/<rest>_<date>.csv (CSV, per restaurant/day)
    4. Support tickets -> data/raw/tickets/tickets.json          (JSON)

Deterministic: a single seeded random.Random instance drives every random
choice, so re-running this script always produces identical output.

Deliberate data-quality issues (missing events, ID-format drift, duplicate
rows, orphaned/null tickets, out-of-order timestamps, null/negative totals,
schema drift) are injected on purpose and are NOT cleaned up here -- that is
the job of the validation stage later in the pipeline.
"""

import csv
import json
import os
import random
import sqlite3
from datetime import datetime, timedelta

# --------------------------------------------------------------------------
# Config -- mirrors the approved synthetic-data plan exactly
# --------------------------------------------------------------------------

SEED = 42
RNG = random.Random(SEED)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "raw")
ORDERS_DIR = os.path.join(RAW_DIR, "orders")
COURIER_DIR = os.path.join(RAW_DIR, "courier")
RESTAURANTS_DIR = os.path.join(RAW_DIR, "restaurants")
TICKETS_DIR = os.path.join(RAW_DIR, "tickets")

N_ORDERS = 400
ORDER_ID_START = 1000
N_RESTAURANTS = 12
N_CUSTOMERS = 150
DATE_START = datetime(2026, 8, 1)
N_DAYS = 14

N_COMPLETED = 340
N_CANCELLED = 32
N_FAILED = 28  # 340 + 32 + 28 = 400

N_CANCELLED_BEFORE_ACCEPT = 16
N_CANCELLED_AFTER_ACCEPT = 16  # 16 + 16 = 32

N_ON_TIME = 238
N_LATE = 102  # 238 + 102 = 340 (completed only)

N_LATE_KITCHEN = 45
N_LATE_COURIER = 36
N_LATE_ASSIGNMENT = 21  # 45 + 36 + 21 = 102

# Restaurants that export a differently-shaped POS file (schema drift).
FORMAT_B_RESTAURANTS = {"R03", "R07", "R11"}

PAYMENT_METHODS = ["card", "upi", "wallet", "cod"]
TICKET_CATEGORIES = ["late_delivery", "wrong_item", "refund", "other"]


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

def ensure_dirs():
    for d in (ORDERS_DIR, COURIER_DIR, RESTAURANTS_DIR, TICKETS_DIR):
        os.makedirs(d, exist_ok=True)


def restaurant_ids():
    return [f"R{str(i).zfill(2)}" for i in range(1, N_RESTAURANTS + 1)]


def customer_ids():
    return [f"CUST-{str(i).zfill(4)}" for i in range(1, N_CUSTOMERS + 1)]


def random_order_time():
    """order_placed_at, weighted toward lunch (12:00-14:30) and dinner (19:00-22:00)."""
    day = DATE_START + timedelta(days=RNG.randint(0, N_DAYS - 1))
    if RNG.random() < 0.6:
        if RNG.random() < 0.5:
            t = day.replace(hour=12, minute=0) + timedelta(minutes=RNG.randint(0, 150))
        else:
            t = day.replace(hour=19, minute=0) + timedelta(minutes=RNG.randint(0, 180))
    else:
        t = day.replace(hour=0, minute=0) + timedelta(minutes=RNG.randint(0, 24 * 60 - 1))
    return t


# --------------------------------------------------------------------------
# Orders (ground-truth timeline shared by all four sources)
# --------------------------------------------------------------------------

def generate_orders():
    """
    Builds the 400-order ground-truth timeline and the bucket assignments
    (on-time/late cause, cancellation stage, failure stage) that every other
    source is derived from. Returns (orders: dict[order_id -> dict], meta: dict[str -> set]).
    """
    order_ids = list(range(ORDER_ID_START, ORDER_ID_START + N_ORDERS))
    RNG.shuffle(order_ids)

    completed_ids = order_ids[:N_COMPLETED]
    cancelled_ids = order_ids[N_COMPLETED:N_COMPLETED + N_CANCELLED]
    failed_ids = order_ids[N_COMPLETED + N_CANCELLED:]

    RNG.shuffle(completed_ids)
    on_time_ids = set(completed_ids[:N_ON_TIME])
    late_ids_list = completed_ids[N_ON_TIME:]
    RNG.shuffle(late_ids_list)
    late_kitchen_ids = set(late_ids_list[:N_LATE_KITCHEN])
    late_courier_ids = set(late_ids_list[N_LATE_KITCHEN:N_LATE_KITCHEN + N_LATE_COURIER])
    late_assignment_ids = set(late_ids_list[N_LATE_KITCHEN + N_LATE_COURIER:])

    RNG.shuffle(cancelled_ids)
    cancelled_before_ids = set(cancelled_ids[:N_CANCELLED_BEFORE_ACCEPT])
    cancelled_after_ids = set(cancelled_ids[N_CANCELLED_BEFORE_ACCEPT:])

    RNG.shuffle(failed_ids)
    half = N_FAILED // 2
    failed_before_pickup_ids = set(failed_ids[:half])
    failed_after_pickup_ids = set(failed_ids[half:])

    rests = restaurant_ids()
    custs = customer_ids()
    completed_set = set(completed_ids)
    cancelled_set = set(cancelled_ids)

    orders = {}
    for oid in order_ids:
        placed_at = random_order_time()
        promised_by = placed_at + timedelta(minutes=RNG.randint(35, 45))
        order = {
            "order_id": oid,
            "customer_id": RNG.choice(custs),
            "restaurant_id": RNG.choice(rests),
            "order_placed_at": placed_at,
            "promised_by": promised_by,
            "payment_method": RNG.choice(PAYMENT_METHODS),
            "order_total": round(RNG.uniform(150, 1200), 2),
            "status": "completed" if oid in completed_set else ("cancelled" if oid in cancelled_set else "failed"),
        }

        if oid in cancelled_before_ids:
            # Cancelled before the restaurant ever accepted it -> no downstream
            # timeline at all, and (later) no restaurant row / no courier events.
            for k in ("accepted_at", "food_ready_at", "assigned_at", "picked_up_at", "en_route_at", "delivered_at", "failed_at"):
                order[k] = None
            orders[oid] = order
            continue

        accepted_at = placed_at + timedelta(minutes=RNG.randint(1, 3))
        order["accepted_at"] = accepted_at

        if oid in cancelled_after_ids:
            # Accepted, then cancelled before the kitchen finished prep ->
            # partial restaurant row, no courier leg at all.
            for k in ("food_ready_at", "assigned_at", "picked_up_at", "en_route_at", "delivered_at", "failed_at"):
                order[k] = None
            orders[oid] = order
            continue

        # Kitchen prep time -- inflated when this order's lateness cause is "kitchen".
        prep_minutes = RNG.randint(25, 50) if oid in late_kitchen_ids else RNG.randint(8, 20)
        food_ready_at = accepted_at + timedelta(minutes=prep_minutes)
        order["food_ready_at"] = food_ready_at

        # Assignment lag -- inflated when the lateness cause is "assignment".
        lag_minutes = RNG.randint(10, 25) if oid in late_assignment_ids else RNG.randint(3, 8)
        assigned_at = food_ready_at + timedelta(minutes=lag_minutes)
        order["assigned_at"] = assigned_at

        if oid in failed_before_pickup_ids:
            # Courier assigned but the delivery attempt was abandoned before pickup.
            order["picked_up_at"] = None
            order["en_route_at"] = None
            order["delivered_at"] = None
            order["failed_at"] = assigned_at + timedelta(minutes=RNG.randint(5, 15))
            orders[oid] = order
            continue

        picked_up_at = assigned_at + timedelta(minutes=RNG.randint(1, 3))
        order["picked_up_at"] = picked_up_at
        order["en_route_at"] = picked_up_at + timedelta(minutes=RNG.randint(2, 5))

        if oid in failed_after_pickup_ids:
            order["delivered_at"] = None
            order["failed_at"] = picked_up_at + timedelta(minutes=RNG.randint(10, 30))
            orders[oid] = order
            continue

        # Courier leg -- inflated when the lateness cause is "courier".
        transit_minutes = RNG.randint(30, 55) if oid in late_courier_ids else RNG.randint(12, 22)
        order["delivered_at"] = picked_up_at + timedelta(minutes=transit_minutes)
        order["failed_at"] = None
        orders[oid] = order

    meta = {
        "on_time_ids": on_time_ids,
        "late_kitchen_ids": late_kitchen_ids,
        "late_courier_ids": late_courier_ids,
        "late_assignment_ids": late_assignment_ids,
        "cancelled_before_ids": cancelled_before_ids,
        "cancelled_after_ids": cancelled_after_ids,
        "failed_before_pickup_ids": failed_before_pickup_ids,
        "failed_after_pickup_ids": failed_after_pickup_ids,
        "completed_ids": completed_set,
        "cancelled_ids": cancelled_set,
        "failed_ids": set(failed_ids),
    }
    return orders, meta


def apply_order_total_issues(orders):
    """6 null order_totals + 5 negative/zero order_totals, disjoint, spread across any status."""
    all_ids = list(orders.keys())
    RNG.shuffle(all_ids)
    null_total_ids = set(all_ids[:6])
    negzero_ids = set(all_ids[6:11])
    for oid in null_total_ids:
        orders[oid]["order_total"] = None
    for oid in negzero_ids:
        orders[oid]["order_total"] = round(RNG.uniform(-60, 0), 2)
    return null_total_ids, negzero_ids


# --------------------------------------------------------------------------
# Source 1: Orders DB (SQLite)
# --------------------------------------------------------------------------

def write_orders_db(orders, duplicate_ids):
    db_path = os.path.join(ORDERS_DIR, "orders.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # order_id is intentionally NOT a primary key / unique constraint -- real
    # operational databases rarely enforce this cleanly, and 8 orders below
    # are inserted twice on purpose (double-insert bug). Detecting and
    # deduping these is left to the validation stage, not this script.
    cur.execute("""
        CREATE TABLE orders (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            customer_id TEXT NOT NULL,
            restaurant_id TEXT NOT NULL,
            order_placed_at TEXT NOT NULL,
            promised_by TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            order_total REAL,
            status TEXT NOT NULL
        )
    """)

    def row_for(order):
        return (
            order["order_id"],
            order["customer_id"],
            order["restaurant_id"],
            order["order_placed_at"].isoformat() + "Z",
            order["promised_by"].isoformat() + "Z",
            order["payment_method"],
            order["order_total"],
            order["status"],
        )

    insert_sql = (
        "INSERT INTO orders (order_id, customer_id, restaurant_id, order_placed_at, "
        "promised_by, payment_method, order_total, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )

    for oid, order in orders.items():
        cur.execute(insert_sql, row_for(order))
        if oid in duplicate_ids:
            # Exact re-insert -- same data, simulating a retry/double-submit bug upstream.
            cur.execute(insert_sql, row_for(order))

    conn.commit()
    conn.close()


# --------------------------------------------------------------------------
# Source 2: Courier Tracking API (JSON events)
# --------------------------------------------------------------------------

def choose_courier_issue_pools(meta):
    """25 missing-delivered, 15 missing-picked_up, 10 out-of-order -- disjoint, from completed orders only."""
    completed_ids = list(meta["completed_ids"])
    RNG.shuffle(completed_ids)
    missing_delivered_ids = set(completed_ids[:25])
    missing_pickedup_ids = set(completed_ids[25:40])
    out_of_order_ids = set(completed_ids[40:50])
    return missing_delivered_ids, missing_pickedup_ids, out_of_order_ids


def generate_courier_events(orders, meta):
    missing_delivered_ids, missing_pickedup_ids, out_of_order_ids = choose_courier_issue_pools(meta)
    courier_pool = [f"C-{i:02d}" for i in range(1, 21)]  # 20 couriers, reused across orders

    events = []
    counter = 1

    def add_event(order_id, event_type, event_time, courier_id):
        nonlocal counter
        events.append({
            "event_id": f"evt_{counter:05d}",
            "order_ref": f"ORD-{order_id}",
            "courier_id": courier_id,
            "event_type": event_type,
            "event_time": event_time.isoformat() + "Z",
        })
        counter += 1

    for oid, order in orders.items():
        if order["status"] not in ("completed", "failed"):
            continue  # cancelled orders never reach courier dispatch in this dataset
        if order["assigned_at"] is None:
            continue  # safety guard, should not trigger for completed/failed

        courier_id = RNG.choice(courier_pool)
        add_event(oid, "assigned", order["assigned_at"], courier_id)

        if order["picked_up_at"] is None:
            # failed_before_pickup: only assigned + failed events exist
            add_event(oid, "failed", order["failed_at"], courier_id)
            continue

        if oid not in missing_pickedup_ids:
            add_event(oid, "picked_up", order["picked_up_at"], courier_id)

        if order["en_route_at"] is not None:
            add_event(oid, "en_route", order["en_route_at"], courier_id)

        if order["status"] == "failed":
            add_event(oid, "failed", order["failed_at"], courier_id)
            continue

        # status == "completed" from here
        if oid in missing_delivered_ids:
            continue  # deliberate gap: "completed" in Orders DB, but courier feed never logged delivery
        elif oid in out_of_order_ids:
            # Deliberate clock-sync glitch: delivered logged a few minutes BEFORE picked_up.
            bad_time = order["picked_up_at"] - timedelta(minutes=RNG.randint(1, 4))
            add_event(oid, "delivered", bad_time, courier_id)
        else:
            add_event(oid, "delivered", order["delivered_at"], courier_id)

    # Deliberate ID-format corruption on 5 events so a naive string-match join drops them.
    mangle_indices = RNG.sample(range(len(events)), 5)
    for idx in mangle_indices:
        oref = events[idx]["order_ref"]
        events[idx]["order_ref"] = oref.replace("ORD-", "ORD_", 1) if RNG.random() < 0.5 else oref[:-1] + "O"

    return events


def write_courier_events(events):
    path = os.path.join(COURIER_DIR, "courier_events.json")
    with open(path, "w") as f:
        json.dump(events, f, indent=2)


# --------------------------------------------------------------------------
# Source 3: Restaurant POS exports (CSV, per restaurant/day)
# --------------------------------------------------------------------------

def choose_restaurant_null_ids(meta):
    """10 rows with a missing timestamp, drawn only from completed/failed (not the naturally-partial cancellations)."""
    pool = list(meta["completed_ids"] | meta["failed_ids"])
    RNG.shuffle(pool)
    return set(pool[:10])


def generate_restaurant_rows(orders, meta):
    null_ts_ids = choose_restaurant_null_ids(meta)
    files = {}  # (restaurant_id, date_str) -> list of row dicts

    for oid, order in orders.items():
        if order["accepted_at"] is None:
            continue  # cancelled-before-accept: no POS record at all

        restaurant_id = order["restaurant_id"]
        date_str = order["order_placed_at"].strftime("%Y-%m-%d")

        accepted_time = order["accepted_at"]
        food_ready_time = order["food_ready_at"]  # None for cancelled-after-accept, present otherwise

        if oid in null_ts_ids:
            if RNG.random() < 0.5:
                accepted_time = None
            else:
                food_ready_time = None

        # Format-B restaurants use a different order-id convention ("FE1000" vs "1000") --
        # a structural convention for that vendor, not random noise.
        row_order_id = f"FE{oid}" if restaurant_id in FORMAT_B_RESTAURANTS else str(oid)

        row = {
            "order_id": row_order_id,
            # No timezone info here (unlike orders/courier, which use "Z") --
            # deliberate representation ambiguity for the pipeline to resolve.
            "accepted_time": accepted_time.strftime("%H:%M:%S") if accepted_time else "",
            "food_ready_time": food_ready_time.strftime("%H:%M:%S") if food_ready_time else "",
        }
        files.setdefault((restaurant_id, date_str), []).append(row)

    return files


def write_restaurant_files(files):
    for (restaurant_id, date_str), rows in files.items():
        filename = f"{restaurant_id.lower()}_{date_str}.csv"
        path = os.path.join(RESTAURANTS_DIR, filename)
        is_format_b = restaurant_id in FORMAT_B_RESTAURANTS
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            if is_format_b:
                writer.writerow(["OrderID", "Accepted_At", "Ready_At", "PrepNotes"])
                for row in rows:
                    writer.writerow([row["order_id"], row["accepted_time"], row["food_ready_time"], ""])
            else:
                writer.writerow(["order_id", "accepted_time", "food_ready_time"])
                for row in rows:
                    writer.writerow([row["order_id"], row["accepted_time"], row["food_ready_time"]])


# --------------------------------------------------------------------------
# Source 4: Support tickets (JSON)
# --------------------------------------------------------------------------

def generate_tickets(orders, meta):
    tickets = []
    counter = 1

    def add_ticket(order_id_field, category, created_at, resolution_status):
        nonlocal counter
        tickets.append({
            "ticket_id": f"T-{8000 + counter}",
            "order_id": order_id_field,
            "category": category,
            "created_at": created_at.isoformat() + "Z",
            "resolution_status": resolution_status,
        })
        counter += 1

    # -- 45 valid tickets, weighted toward late and failed orders --
    late_pool = list(meta["late_kitchen_ids"] | meta["late_courier_ids"] | meta["late_assignment_ids"])
    failed_pool = list(meta["failed_ids"])
    other_pool = list(meta["on_time_ids"] | meta["cancelled_ids"])
    RNG.shuffle(late_pool)
    RNG.shuffle(failed_pool)
    RNG.shuffle(other_pool)

    valid_sources = (
        [(oid, "late_delivery") for oid in late_pool[:30]]
        + [(oid, RNG.choice(["refund", "other"])) for oid in failed_pool[:10]]
        + [(oid, RNG.choice(TICKET_CATEGORIES)) for oid in other_pool[:5]]
    )
    for oid, category in valid_sources:
        order = orders[oid]
        base_time = order["delivered_at"] or order["failed_at"] or order["order_placed_at"]
        created_at = base_time + timedelta(minutes=RNG.randint(5, 120))
        add_ticket(str(oid), category, created_at, RNG.choice(["open", "resolved", "refunded"]))

    # -- 12 orphaned tickets: order_id that was never a real order --
    for _ in range(12):
        fake_id = RNG.randint(9000, 9999)
        created_at = DATE_START + timedelta(days=RNG.randint(0, N_DAYS - 1), minutes=RNG.randint(0, 24 * 60 - 1))
        add_ticket(str(fake_id), RNG.choice(TICKET_CATEGORIES), created_at, RNG.choice(["open", "resolved"]))

    # -- 8 tickets with a missing order_id entirely --
    for _ in range(8):
        created_at = DATE_START + timedelta(days=RNG.randint(0, N_DAYS - 1), minutes=RNG.randint(0, 24 * 60 - 1))
        add_ticket(None, RNG.choice(TICKET_CATEGORIES), created_at, "open")

    RNG.shuffle(tickets)
    return tickets


def write_tickets(tickets):
    path = os.path.join(TICKETS_DIR, "tickets.json")
    with open(path, "w") as f:
        json.dump(tickets, f, indent=2)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    ensure_dirs()

    orders, meta = generate_orders()
    null_total_ids, negzero_ids = apply_order_total_issues(orders)

    all_ids = list(orders.keys())
    RNG.shuffle(all_ids)
    duplicate_ids = set(all_ids[:8])
    write_orders_db(orders, duplicate_ids)

    courier_events = generate_courier_events(orders, meta)
    write_courier_events(courier_events)

    restaurant_files = generate_restaurant_rows(orders, meta)
    write_restaurant_files(restaurant_files)
    restaurant_row_count = sum(len(rows) for rows in restaurant_files.values())

    tickets = generate_tickets(orders, meta)
    write_tickets(tickets)

    print("=" * 60)
    print(f"Synthetic FlashEats data generated (seed={SEED})")
    print("=" * 60)
    print(f"Orders DB:       {os.path.join(ORDERS_DIR, 'orders.db')}")
    print(f"  - {N_ORDERS} orders ({N_COMPLETED} completed / {N_CANCELLED} cancelled / {N_FAILED} failed)")
    print(f"  - {len(duplicate_ids)} orders duplicated (exact re-insert)")
    print(f"  - {len(null_total_ids)} null order_total, {len(negzero_ids)} negative/zero order_total")
    print()
    print(f"Courier events:  {os.path.join(COURIER_DIR, 'courier_events.json')}")
    print(f"  - {len(courier_events)} events across {len(orders)} orders")
    print()
    print(f"Restaurant POS:  {RESTAURANTS_DIR}/*.csv")
    print(f"  - {restaurant_row_count} order rows across {len(restaurant_files)} files "
          f"({N_RESTAURANTS} restaurants, {len(FORMAT_B_RESTAURANTS)} using the drifted schema)")
    print()
    print(f"Support tickets: {os.path.join(TICKETS_DIR, 'tickets.json')}")
    print(f"  - {len(tickets)} tickets (12 orphaned order_id, 8 null order_id, 45 linked to real orders)")
    print("=" * 60)


if __name__ == "__main__":
    main()