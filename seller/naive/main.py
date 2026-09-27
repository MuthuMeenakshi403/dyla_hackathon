"""
Naive ticket-selling API — INTENTIONALLY unsafe.

State lives in a plain Python dict, there is no locking, and the
buy() logic is a classic check-then-act race condition. This is the
"before" version, meant to be hammered with concurrent requests to
prove it oversells tickets before a fixed version is built.
"""

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Naive Ticket API (v1 - broken by design)")

# --- Global in-memory state -------------------------------------------
# A single shared dict, mutated directly by every request handler.
# No database, no lock, no transaction — just a dict living in the
# process's memory. Every worker/thread that handles a request reads
# and writes this same object with zero coordination.
state = {
    "capacity": 0,
    "sold": 0,
    "user_tickets": {},  # user_id -> list[int] of ticket numbers
}


class ResetRequest(BaseModel):
    ticket_count: int


class BuyRequest(BaseModel):
    user_id: str
    request_id: str  # accepted but deliberately NOT used for dedup


@app.post("/reset")
def reset(req: ResetRequest):
    """
    Wipes all state and starts a fresh sale with `ticket_count` tickets
    available. No confirmation, no versioning — just overwrite the dict.
    """
    state["capacity"] = req.ticket_count
    state["sold"] = 0
    state["user_tickets"] = {}
    return {"message": "reset", "capacity": state["capacity"]}


@app.post("/buy")
def buy(req: BuyRequest):
    """
    The core race condition. This does a "check, then act":
        1. Read state["sold"] and compare to state["capacity"]
        2. If there's room, increment state["sold"] and hand out a ticket

    Between step 1 and step 2 there is a window where another request
    (on another thread/worker, or even just interleaved via async)
    can read the SAME "sold" value, see room too, and also issue a
    ticket. Under concurrent load, sold can blow past capacity.

    request_id is accepted (as a real API would use it for idempotent
    retries) but intentionally ignored here — this version will also
    double-book a user who retries the same request_id concurrently.
    """
    if state["sold"] < state["capacity"]:
        state["sold"] += 1
        ticket_number = state["sold"]
        state["user_tickets"].setdefault(req.user_id, []).append(ticket_number)
        return {"status": "success", "ticket_number": ticket_number}
    else:
        return {"status": "sold_out"}


@app.get("/status")
def status():
    """
    Read-only snapshot of current state: how many tickets have been
    sold, total capacity, and the full user_id -> ticket numbers map.
    Useful for a test harness to check sold count vs. len(all tickets
    issued) — where the naive version's bugs will show up as a mismatch.
    """
    return {
        "sold": state["sold"],
        "capacity": state["capacity"],
        "user_tickets": state["user_tickets"],
    }


# Run with: uvicorn main:app --reload
