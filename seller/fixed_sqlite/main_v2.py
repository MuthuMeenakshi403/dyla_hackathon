"""
Ticket-selling API v2 — atomic SQLite fix for the race conditions
proven by load_test.py against the naive (dict-based) v1.

Fixes:
  1. Oversold tickets: the capacity check and the ticket insert are
     now one INSERT...SELECT...WHERE statement, not a separate
     read-then-write. SQLite serializes writers, so there is no
     window between "check" and "act" for another request to land in.
  2. Duplicate request_id issuing two tickets: request_id has a
     UNIQUE constraint, and the same WHERE clause also rejects a
     request_id that already has a ticket. A retried request_id gets
     back its original ticket instead of a new one.
"""

import sqlite3
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DB_PATH = Path(__file__).parent / "tickets.db"

app = FastAPI(title="Ticket API v2 (atomic SQLite)")


def get_conn():
    # timeout=30 sets SQLite's busy_timeout: if the DB is locked by
    # another writer, this connection waits (up to 30s) instead of
    # immediately raising "database is locked".
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")  # readers don't block the writer, and vice versa
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
    """
    Wipes all tickets and sets a fresh capacity. DELETE + UPDATE run
    in one transaction so /buy can never observe a half-reset state
    (old tickets cleared but capacity not yet updated, or vice versa).
    Ticket numbers restart at 1: plain (non-AUTOINCREMENT) rowids
    reuse from 1 once the table is empty.
    """
    if req.ticket_count < 0:
        raise HTTPException(status_code=400, detail="Ticket count cannot be negative.")

    conn = get_conn()
    try:
        conn.execute("DELETE FROM tickets")
        conn.execute("UPDATE meta SET capacity = ? WHERE id = 1", (req.ticket_count,))
        conn.commit()
    finally:
        conn.close()
    return {"message": "reset", "capacity": req.ticket_count}


@app.post("/buy")
def buy(req: BuyRequest):
    """
    The atomic check-and-issue. See module docstring / chat explanation
    for why the single INSERT...SELECT...WHERE below closes the race
    that the naive version had.
    """
    if not req.user_id.strip() or not req.request_id.strip():
        raise HTTPException(status_code=400, detail="user_id and request_id are required.")

    conn = get_conn()
    try:
        cur = conn.execute(
            """
            INSERT INTO tickets (user_id, request_id)
            SELECT ?, ?
            WHERE (SELECT COUNT(*) FROM tickets) < (SELECT capacity FROM meta WHERE id = 1)
              AND NOT EXISTS (SELECT 1 FROM tickets WHERE request_id = ?)
            """,
            (req.user_id, req.request_id, req.request_id),
        )
        if cur.rowcount == 1:
            conn.commit()
            return {"status": "success", "ticket_number": cur.lastrowid}

        conn.rollback()
        # The atomic insert above did NOT run (WHERE was false). This
        # follow-up read is just to pick the right response — it plays
        # no part in the atomicity guarantee, since the outcome was
        # already fixed by the statement above.
        existing = conn.execute(
            "SELECT ticket_number FROM tickets WHERE request_id = ?",
            (req.request_id,),
        ).fetchone()
        if existing:
            # Idempotent replay of an already-fulfilled request_id.
            return {"status": "success", "ticket_number": existing[0]}
        return {"status": "sold_out"}
    finally:
        conn.close()


@app.get("/status")
def status():
    """Read-only snapshot: sold count, capacity, and user_id -> tickets."""
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


# Run with: uvicorn main_v2:app --reload