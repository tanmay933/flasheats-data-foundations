"""
src/ingestion/ingest.py

Ingestion layer for the FlashEats pipeline.

Retrieves raw data from all four simulated client systems and returns it
as-is: no cleaning, deduplication, ID normalization, or business-rule
fixes. That is the job of src/validation and src/transformation, which run
after this module.

Retrieval modes (assignment requirement: at least two across SQL/API/files):

    - Orders            -> SQL      (sqlite3 against data/raw/orders/orders.db)
    - Courier events    -> API      (requests, via a local mock transport
                                      adapter -- no real server is started)
    - Restaurant POS    -> Files    (CSV, one file per restaurant/day)
    - Support tickets   -> Files    (JSON)

Every ingest_* function preserves the raw source untouched (read-only) and
logs how many records it retrieved, so retrieval completeness can be
demonstrated later (e.g. in the README / demo).
"""

import csv
import glob
import json
import logging
import os
import sqlite3

import requests

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ingestion")

# --------------------------------------------------------------------------
# Paths (relative to this file, independent of the caller's working dir)
# --------------------------------------------------------------------------

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "..", ".."))

RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
ORDERS_DB_PATH = os.path.join(RAW_DIR, "orders", "orders.db")
COURIER_EVENTS_PATH = os.path.join(RAW_DIR, "courier", "courier_events.json")
RESTAURANTS_DIR = os.path.join(RAW_DIR, "restaurants")
TICKETS_PATH = os.path.join(RAW_DIR, "tickets", "tickets.json")

# Documents which retrieval mode each source uses -- referenced in the
# summary log so the "at least two retrieval modes" requirement is visible
# at runtime, not just in the README.
RETRIEVAL_MODES = {
    "orders": "SQL (sqlite3)",
    "courier_events": "API (requests, mock transport adapter)",
    "restaurant_pos": "Files (CSV)",
    "support_tickets": "Files (JSON)",
}


class IngestionError(Exception):
    """Raised when a raw source cannot be retrieved at all (missing file, bad DB, etc.)."""
    pass


# --------------------------------------------------------------------------
# Source 1: Orders -- SQL
# --------------------------------------------------------------------------

def ingest_orders(db_path=ORDERS_DB_PATH):
    """
    Retrieves every row from the orders table, exactly as stored -- including
    the deliberate duplicate rows and null/negative order_total values.
    No dedup, no filtering, no normalization here.
    """
    if not os.path.exists(db_path):
        raise IngestionError(f"Orders DB not found at {db_path}")

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders")
        rows = [dict(row) for row in cur.fetchall()]
        conn.close()
    except sqlite3.Error as e:
        raise IngestionError(f"Failed to read orders DB at {db_path}: {e}") from e

    logger.info(f"[{RETRIEVAL_MODES['orders']}] Retrieved {len(rows)} order rows from {db_path}")
    return rows


# --------------------------------------------------------------------------
# Source 2: Courier events -- API (via a local mock transport adapter)
# --------------------------------------------------------------------------

class _MockCourierAPIAdapter(requests.adapters.BaseAdapter):
    """
    A `requests` transport adapter that intercepts calls to the fake
    "mockapi://" scheme and serves them from the local courier_events.json
    file, wrapped in a real requests.Response object.

    This lets ingest_courier_events() use the actual `requests` API
    (session.get(), response.json(), response.raise_for_status(), etc.)
    the same way it would against a real courier-tracking service, without
    running an actual HTTP server -- appropriate for a student project
    simulating a third-party API.
    """

    def __init__(self, events_path):
        super().__init__()
        self._events_path = events_path

    def send(self, request, **kwargs):
        response = requests.models.Response()
        response.status_code = 200
        response.url = request.url
        response.request = request

        if not os.path.exists(self._events_path):
            response.status_code = 404
            response._content = json.dumps(
                {"error": f"courier events source not found at {self._events_path}"}
            ).encode("utf-8")
            return response

        try:
            with open(self._events_path, "r") as f:
                events = json.load(f)
        except json.JSONDecodeError as e:
            response.status_code = 502
            response._content = json.dumps({"error": f"malformed courier events source: {e}"}).encode("utf-8")
            return response

        # Optional "?since=<ISO timestamp>" filtering, mimicking a real
        # paginated/incremental API. Ingestion still retrieves everything
        # by default -- callers opt into filtering explicitly.
        since = request.url.split("since=")[1] if "since=" in request.url else None
        if since:
            events = [e for e in events if e.get("event_time", "") >= since]

        response._content = json.dumps(events).encode("utf-8")
        return response

    def close(self):
        pass


def _mock_api_session(events_path=COURIER_EVENTS_PATH):
    session = requests.Session()
    session.mount("mockapi://", _MockCourierAPIAdapter(events_path))
    return session


def ingest_courier_events(events_path=COURIER_EVENTS_PATH, since=None):
    """
    Retrieves courier tracking events through an API-style call
    (session.get -> response.json()), including the deliberately mangled
    order_ref values and any missing-event gaps. No ID normalization or
    event reconciliation happens here.
    """
    session = _mock_api_session(events_path)
    url = "mockapi://courier-service/v1/events"
    if since:
        url += f"?since={since}"

    try:
        response = session.get(url, timeout=5)
    except requests.RequestException as e:
        raise IngestionError(f"Courier API request failed: {e}") from e

    if response.status_code != 200:
        raise IngestionError(
            f"Courier API returned status {response.status_code}: {response.text}"
        )

    events = response.json()
    logger.info(
        f"[{RETRIEVAL_MODES['courier_events']}] Retrieved {len(events)} courier events "
        f"from {events_path}"
    )
    return events


# --------------------------------------------------------------------------
# Source 3: Restaurant POS exports -- Files (CSV)
# --------------------------------------------------------------------------

def ingest_restaurant_pos(restaurants_dir=RESTAURANTS_DIR):
    """
    Reads every restaurant POS CSV file as-is. Different restaurants export
    different column layouts (schema drift) -- this function reads whatever
    columns exist per file (via DictReader) rather than forcing a common
    schema. Each row is tagged with its source filename for traceability
    only; no field values are renamed, reformatted, or normalized.
    """
    if not os.path.isdir(restaurants_dir):
        raise IngestionError(f"Restaurant POS directory not found at {restaurants_dir}")

    csv_paths = sorted(glob.glob(os.path.join(restaurants_dir, "*.csv")))
    if not csv_paths:
        raise IngestionError(f"No restaurant POS CSV files found in {restaurants_dir}")

    all_rows = []
    per_file_counts = {}

    for path in csv_paths:
        try:
            with open(path, "r", newline="") as f:
                reader = csv.DictReader(f)
                file_rows = list(reader)
        except (OSError, csv.Error) as e:
            raise IngestionError(f"Failed to read restaurant POS file {path}: {e}") from e

        for row in file_rows:
            row["_source_file"] = os.path.basename(path)
        all_rows.extend(file_rows)
        per_file_counts[os.path.basename(path)] = len(file_rows)

    logger.info(
        f"[{RETRIEVAL_MODES['restaurant_pos']}] Retrieved {len(all_rows)} rows "
        f"across {len(csv_paths)} restaurant files from {restaurants_dir}"
    )
    logger.info(f"  Per-file row counts: {per_file_counts}")
    return all_rows


# --------------------------------------------------------------------------
# Source 4: Support tickets -- Files (JSON)
# --------------------------------------------------------------------------

def ingest_support_tickets(tickets_path=TICKETS_PATH):
    """
    Reads all support tickets as-is, including orphaned order_id values
    and null order_id entries. No linkage to orders happens here.
    """
    if not os.path.exists(tickets_path):
        raise IngestionError(f"Support tickets file not found at {tickets_path}")

    try:
        with open(tickets_path, "r") as f:
            tickets = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise IngestionError(f"Failed to read support tickets file {tickets_path}: {e}") from e

    logger.info(
        f"[{RETRIEVAL_MODES['support_tickets']}] Retrieved {len(tickets)} support tickets "
        f"from {tickets_path}"
    )
    return tickets


# --------------------------------------------------------------------------
# Orchestration -- single entry point for run_pipeline.py
# --------------------------------------------------------------------------

def ingest_all():
    """
    Runs all four ingestion functions and returns their raw output together,
    plus a retrieval summary. Any individual source failure raises
    IngestionError with a clear message rather than failing silently or
    returning partial/guessed data.

    Returns:
        {
            "orders": [...],
            "courier_events": [...],
            "restaurant_pos": [...],
            "support_tickets": [...],
            "summary": {source_name: {"mode": ..., "count": ...}, ...},
        }
    """
    logger.info("Starting ingestion from all four raw sources...")

    orders = ingest_orders()
    courier_events = ingest_courier_events()
    restaurant_pos = ingest_restaurant_pos()
    support_tickets = ingest_support_tickets()

    summary = {
        "orders": {"mode": RETRIEVAL_MODES["orders"], "count": len(orders)},
        "courier_events": {"mode": RETRIEVAL_MODES["courier_events"], "count": len(courier_events)},
        "restaurant_pos": {"mode": RETRIEVAL_MODES["restaurant_pos"], "count": len(restaurant_pos)},
        "support_tickets": {"mode": RETRIEVAL_MODES["support_tickets"], "count": len(support_tickets)},
    }

    logger.info("Ingestion complete. Summary:")
    for source, info in summary.items():
        logger.info(f"  - {source}: {info['count']} records via {info['mode']}")

    return {
        "orders": orders,
        "courier_events": courier_events,
        "restaurant_pos": restaurant_pos,
        "support_tickets": support_tickets,
        "summary": summary,
    }


if __name__ == "__main__":
    # Manual smoke-test: run `python -m src.ingestion.ingest` from the
    # project root to confirm all four sources are retrievable.
    ingest_all()