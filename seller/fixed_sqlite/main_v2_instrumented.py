"""
Diagnostic build of the atomic ticket seller. Same /buy logic as
main_v2.py, but every request records timestamps at each stage:

    received  -> handler_start -> connected -> pre_counted -> inserted -> committed -> closed

  queue_wait_ms : received -> handler_start   (ASGI/thread-pool dispatch delay)
  connect_ms    : handler_start -> connected  (sqlite3.connect + PRAGMA)
  precount_ms   : connected -> pre_counted    (a DIAGNOSTIC-ONLY extra
                  `SELECT COUNT(*) FROM tickets`, run just to log the
                  table size seen by this request and its cost in
                  isolation — production main_v2.py doesn't do this
                  extra read, so it has slightly less overhead than
                  this build)
  insert_ms     : pre_counted -> inserted     (the real atomic
                  INSERT...SELECT...WHERE statement — this is where
                  both "SQLite lock wait" and "COUNT(*) scan cost"
                  actually live)
  commit_ms     : inserted -> committed        (commit/rollback, incl.
                  WAL fsync)
  total_ms      : received -> closed

GET /debug/timings returns percentiles for each stage plus a bucketed
view of insert_ms against table size at the time of the call, which is
what actually distinguishes "the table got bigger so the check got
slower" (O(n) COUNT(*) scan) from "everyone's just waiting their turn"
(pure lock serialization, cost independent of table size).
"""

import sqlite3
import statistics
import threading
import time
from pathlib import Path

from fastapi import FastAPI, Request
from pydantic import BaseModel

DB_PATH = Path(__file__).parent / "tickets_instrumented.db"

app = FastAPI(title="Ticket API v2 (instrumented)")

timings_lock = threading.Lock()
timings = []


@app.middleware("http")
async def stamp_arrival(request: Request, call_next):
    # Runs on the event loop, BEFORE the sync handler is dispatched to
    # the thread pool — this timestamp is the true "request arrived" mark.
    request.state.received_at = time.perf_counter()
    return await call_next(request)


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = get_conn()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                capacity INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                ticket_number INTEGER PRIMARY KEY,
                user_id TEXT NOT NULL,
                request_id TEXT NOT NULL UNIQUE
            )
        """)
        conn.execute("INSERT OR IGNORE INTO meta (id, capacity) VALUES (1, 0)")
        conn.commit()
    finally:
        conn.close()


init_db()


class ResetRequest(BaseModel):
    ticket_count: int


class BuyRequest(BaseModel):
    user_id: str
    request_id: str


@app.post("/reset")
def reset(req: ResetRequest):
    conn = get_conn()
    try:
        conn.execute("DELETE FROM tickets")
        conn.execute("UPDATE meta SET capacity = ? WHERE id = 1", (req.ticket_count,))
        conn.commit()
    finally:
        conn.close()
    with timings_lock:
        timings.clear()
    return {"message": "reset", "capacity": req.ticket_count}


@app.post("/buy")
def buy(req: BuyRequest, request: Request):
    received_at = request.state.received_at
    handler_start = time.perf_counter()

    conn = get_conn()
    connected = time.perf_counter()

    # Diagnostic-only: measure the table-size-check in isolation.
    current_count = conn.execute("SELECT COUNT(*) FROM tickets").fetchone()[0]
    pre_counted = time.perf_counter()

    cur = conn.execute(
        """
        INSERT INTO tickets (user_id, request_id)
        SELECT ?, ?
        WHERE (SELECT COUNT(*) FROM tickets) < (SELECT capacity FROM meta WHERE id = 1)
          AND NOT EXISTS (SELECT 1 FROM tickets WHERE request_id = ?)
        """,
        (req.user_id, req.request_id, req.request_id),
    )
    inserted = time.perf_counter()

    if cur.rowcount == 1:
        conn.commit()
        committed = time.perf_counter()
        ticket_number = cur.lastrowid
        outcome = "success"
    else:
        conn.rollback()
        committed = time.perf_counter()
        existing = conn.execute(
            "SELECT ticket_number FROM tickets WHERE request_id = ?", (req.request_id,)
        ).fetchone()
        if existing:
            ticket_number = existing[0]
            outcome = "success"
        else:
            ticket_number = None
            outcome = "sold_out"

    conn.close()
    closed = time.perf_counter()

    with timings_lock:
        timings.append({
            "queue_wait_ms": (handler_start - received_at) * 1000,
            "connect_ms": (connected - handler_start) * 1000,
            "precount_ms": (pre_counted - connected) * 1000,
            "insert_ms": (inserted - pre_counted) * 1000,
            "commit_ms": (committed - inserted) * 1000,
            "close_ms": (closed - committed) * 1000,
            "total_ms": (closed - received_at) * 1000,
            "table_size_at_start": current_count,
        })

    if outcome == "success":
        return {"status": "success", "ticket_number": ticket_number}
    return {"status": "sold_out"}


@app.get("/status")
def status():
    conn = get_conn()
    try:
        capacity = conn.execute("SELECT capacity FROM meta WHERE id = 1").fetchone()[0]
        rows = conn.execute(
            "SELECT user_id, ticket_number FROM tickets ORDER BY ticket_number"
        ).fetchall()
    finally:
        conn.close()
    user_tickets = {}
    for user_id, ticket_number in rows:
        user_tickets.setdefault(user_id, []).append(ticket_number)
    return {"sold": len(rows), "capacity": capacity, "user_tickets": user_tickets}


def pct(vals, p):
    if not vals:
        return 0.0
    vals = sorted(vals)
    k = (len(vals) - 1) * p
    f, c = int(k), min(int(k) + 1, len(vals) - 1)
    return vals[f] if f == c else vals[f] + (vals[c] - vals[f]) * (k - f)


@app.get("/debug/timings")
def debug_timings():
    with timings_lock:
        snapshot = list(timings)

    if not snapshot:
        return {"count": 0}

    fields = ["queue_wait_ms", "connect_ms", "precount_ms", "insert_ms", "commit_ms", "close_ms", "total_ms"]
    breakdown = {}
    for f in fields:
        vals = [t[f] for t in snapshot]
        breakdown[f] = {
            "p50": round(statistics.median(vals), 2),
            "p99": round(pct(vals, 0.99), 2),
            "mean": round(statistics.mean(vals), 2),
        }

    # Does insert_ms grow with table size? Bucket requests by table
    # size seen (first third of the sale vs last third) and compare
    # mean insert_ms. If it grows substantially, that's the O(n)
    # COUNT(*) scan cost showing up, not flat lock-wait.
    by_size = sorted(snapshot, key=lambda t: t["table_size_at_start"])
    n = len(by_size)
    first_third = by_size[: n // 3] or by_size
    last_third = by_size[-(n // 3):] if n // 3 else by_size
    size_correlation = {
        "first_third_mean_insert_ms": round(statistics.mean(t["insert_ms"] for t in first_third), 2),
        "first_third_mean_table_size": round(statistics.mean(t["table_size_at_start"] for t in first_third), 1),
        "last_third_mean_insert_ms": round(statistics.mean(t["insert_ms"] for t in last_third), 2),
        "last_third_mean_table_size": round(statistics.mean(t["table_size_at_start"] for t in last_third), 1),
    }

    return {"count": n, "breakdown_ms": breakdown, "insert_ms_vs_table_size": size_correlation}


# Run with: uvicorn main_v2_instrumented:app --reload
