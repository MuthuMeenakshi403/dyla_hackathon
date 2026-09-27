"""
Ticket-Selling API - NAIVE / UNSAFE VERSION (for demonstrating the race)

Same three endpoints, same response shapes as the working, lock-protected
main.py, so the existing load_test.py can run against this file with zero
changes — just point --base-url at whichever server you started.

The ONLY intentional differences from main.py:
  1. The asyncio.Lock is gone.
  2. The idempotency-cache check and the capacity check are each split
     into a separate "check, then act" pair with nothing tying them
     together atomically — "the simple obvious way" someone would write
     this before thinking about concurrency.

IMPORTANT — why the `await asyncio.sleep(0)` calls are here:
A single-process asyncio event loop only switches between concurrent
requests at an `await` point. An `async def` handler with NO `await`
inside it runs to completion without ever being interrupted — so two
"concurrent" HTTP requests would never actually interleave their
execution, and this race would essentially never surface no matter how
many buyers you fire at once, lock or no lock. The sleep(0) calls below
stand in for the gap that exists in almost any real system between
"check" and "act" (a network hop, a second database round-trip, a cache
lookup elsewhere) — they're what actually opens the race window here.
Remove them and you're just testing your load generator's timing luck;
keep them and the demo is reliable.
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio

app = FastAPI(title="Ticket Stampede API - Naive/Unsafe Version")

# --- Global In-Memory State (deliberately NOT guarded by a lock) ---
state = {
    "capacity": 0,
    "sold": 0,
    "user_tickets": {},       # user_id -> list[int] of ticket numbers
    "request_cache": {}       # request_id -> {"ticket_number": int, "user_id": str}
}


class ResetRequest(BaseModel):
    ticket_count: int


class BuyRequest(BaseModel):
    user_id: str
    request_id: str


@app.post("/reset")
async def reset(req: ResetRequest):
    """Wipes all state and starts a fresh sale with `ticket_count` available."""
    if req.ticket_count < 0:
        raise HTTPException(status_code=400, detail="Ticket count cannot be negative.")

    state["capacity"] = req.ticket_count
    state["sold"] = 0
    state["user_tickets"] = {}
    state["request_cache"] = {}
    return {"message": "reset", "capacity": state["capacity"]}


@app.post("/buy")
async def buy(req: BuyRequest):
    """
    NAIVE, UNSAFE buy endpoint — no lock. The idempotency check and the
    capacity check are each a separate "check, then act" pair, exactly
    like a first-draft implementation would look before anyone thought
    about what happens when two requests land at the same instant.
    """
    if not req.user_id or not req.request_id:
        raise HTTPException(status_code=400, detail="user_id and request_id are required.")

    # --- Idempotency check (step 1: "check") ---
    if req.request_id in state["request_cache"]:
        cached = state["request_cache"][req.request_id]
        return {
            "status": "success",
            "ticket_number": cached["ticket_number"],
            "idempotent_replay": True
        }

    # The race window for a "concurrent idempotency storm": two requests
    # with the SAME request_id can both reach here (both missed the
    # cache above) before either one has written to request_cache.
    await asyncio.sleep(0.02)

    # --- Capacity check (step 1: "check") ---
    if state["sold"] < state["capacity"]:
        # The classic TOCTOU window: any number of requests can pass this
        # check while sold is still under capacity, before any of them
        # gets to the increment below.
        await asyncio.sleep(0.02)

        # --- Capacity increment (step 2: "act" — NOT atomic with the check above) ---
        state["sold"] += 1
        ticket_number = state["sold"]

        state["user_tickets"].setdefault(req.user_id, []).append(ticket_number)
        state["request_cache"][req.request_id] = {
            "ticket_number": ticket_number,
            "user_id": req.user_id
        }

        return {"status": "success", "ticket_number": ticket_number}
    else:
        return {"status": "sold_out"}


@app.get("/status")
async def status():
    """Snapshot of current state — no lock here either, but as a read-only
    endpoint that's a stale-read risk, not an overselling risk."""
    return {
        "sold": state["sold"],
        "capacity": state["capacity"],
        "user_tickets": state["user_tickets"],
    }


# Run with: uvicorn main_naive:app --host 127.0.0.1 --port 8001
# (run your working main.py on a different port, e.g. 8000, and point
#  load_test.py's --base-url at each in turn — no other changes needed)
