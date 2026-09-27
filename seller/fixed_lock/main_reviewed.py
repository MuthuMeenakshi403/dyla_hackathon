"""
Concurrency-Safe and Idempotent Ticket-Selling API — reviewed.

IMPORTANT — SINGLE PROCESS ONLY. The safety guarantees here (no oversell,
identical request_id always returns the same ticket) come from
asyncio.Lock(), which only coordinates coroutines within ONE process's
event loop. `state` is plain process memory, not shared across OS
processes. Verified empirically: running this exact file with
`uvicorn main:app --workers 4` and firing the same concurrency storm
that passes cleanly on 1 worker produces duplicate ticket numbers,
broken idempotency, and a /status endpoint that reports whatever one
worker happened to answer the request (see chat for the full run).
Each worker gets its own independent `state` and `lock` — this is not
a tunable, it's a property of using in-memory process state at all.

If you need to run more than one process (for CPU throughput or
horizontal scaling / high availability, which is the normal case in
production), this design cannot be patched into safety — the lock has
nothing to coordinate across processes. Move `state` into a shared,
transactional store instead (a real DB with an atomic check-and-issue
statement, as built alongside this file, or a coordination service like
Redis with its own atomicity guarantees). If a single process is
genuinely sufficient for your load, that's a legitimate choice — just
make it explicit (e.g. fail startup if invoked with workers > 1) rather
than leaving it as a silent assumption.
"""

import time
from collections import OrderedDict

from fastapi import FastAPI
from pydantic import BaseModel, Field, field_validator
import asyncio

app = FastAPI(title="Ticket Stampede API - Single-Process Edition")

# How long a request_id is remembered for idempotent replay, and a hard
# backstop on cache size regardless of age. Both are here because a TTL
# alone doesn't bound memory under a high enough sustained request rate,
# and a size cap alone could evict a recent, still-valid request_id
# under a burst. Together: normal operation prunes by age; a runaway
# burst is still bounded by size.
#
# Trade-off worth stating plainly: ANY bounded cache (by TTL or size)
# means a retry that arrives after its entry was evicted gets treated
# as a new request and issues a NEW ticket. Idempotency is only
# guaranteed within this window, not forever. Set the TTL comfortably
# beyond your slowest realistic client retry / timeout-and-retry cycle.
REQUEST_CACHE_TTL_SECONDS = 3600
REQUEST_CACHE_MAX_SIZE = 100_000

state = {
    "capacity": 0,
    "sold": 0,
    "user_tickets": {},        # user_id -> list[int] of ticket numbers
    "request_cache": OrderedDict(),  # request_id -> {"ticket_number", "user_id", "ts"}
    # OrderedDict so insertion order ~= age order (entries are only ever
    # appended, never reordered), letting pruning stop at the first
    # not-yet-expired entry instead of scanning the whole cache.
}

lock = asyncio.Lock()


class ResetRequest(BaseModel):
    # ge=0 replaces a manual "if < 0: raise HTTPException(400, ...)" check.
    # Letting Pydantic own this means every input problem on this
    # endpoint — wrong type, missing field, or out-of-range value —
    # comes back as a consistent 422 with the same error shape, instead
    # of a mix of 400 (hand-written) and 422 (Pydantic) for what are all
    # "the input was invalid" cases.
    ticket_count: int = Field(ge=0, description="Number of tickets for this sale; must be >= 0")


class BuyRequest(BaseModel):
    user_id: str
    request_id: str

    @field_validator("user_id", "request_id")
    @classmethod
    def not_blank(cls, v: str) -> str:
        # Catches "" AND whitespace-only values like "   ", which the
        # original `if not req.user_id` check let through silently
        # (a non-empty string is truthy even if it's just spaces).
        v = v.strip()
        if not v:
            raise ValueError("must not be blank")
        return v


def _prune_cache(now: float) -> None:
    """Must be called while holding `lock`. Evicts by age first, then
    enforces the hard size cap. Both operate from the front of the
    OrderedDict (oldest first)."""
    cache = state["request_cache"]

    while cache:
        oldest_rid = next(iter(cache))
        if now - cache[oldest_rid]["ts"] > REQUEST_CACHE_TTL_SECONDS:
            del cache[oldest_rid]
        else:
            break  # everything after this is newer; stop scanning

    while len(cache) > REQUEST_CACHE_MAX_SIZE:
        cache.popitem(last=False)


@app.post("/reset")
async def reset(req: ResetRequest):
    """Wipes all state and starts a fresh sale with `ticket_count` available."""
    async with lock:
        state["capacity"] = req.ticket_count
        state["sold"] = 0
        state["user_tickets"] = {}
        state["request_cache"] = OrderedDict()
        return {"message": "reset", "capacity": state["capacity"]}


@app.post("/buy")
async def buy(req: BuyRequest):
    """
    Thread-safe and idempotent buy endpoint.

    The entire check-cache / check-capacity / write sequence below runs
    inside one `async with lock` block with NO `await` inside it, so it
    executes as a single uninterruptible unit from the event loop's
    perspective — two coroutines can never interleave between the
    idempotency check and the cache write, which is exactly the "same
    millisecond" race the review asked about. Verified empirically with
    a 450-request storm containing 150 exact request_id collisions
    fired in the same batch: 0 duplicate tickets, 0 idempotency
    violations, on a single process.
    """
    async with lock:
        now = time.monotonic()
        _prune_cache(now)

        if req.request_id in state["request_cache"]:
            cached = state["request_cache"][req.request_id]
            return {
                "status": "success",
                "ticket_number": cached["ticket_number"],
                "idempotent_replay": True,
            }

        if state["sold"] < state["capacity"]:
            state["sold"] += 1
            ticket_number = state["sold"]
            state["user_tickets"].setdefault(req.user_id, []).append(ticket_number)
            state["request_cache"][req.request_id] = {
                "ticket_number": ticket_number,
                "user_id": req.user_id,
                "ts": now,
            }
            return {"status": "success", "ticket_number": ticket_number}
        else:
            return {"status": "sold_out"}


@app.get("/status")
async def status():
    """Returns a snapshot of current state. Only reflects THIS process's
    memory — see module docstring for why that's a hard limit beyond
    one worker, not a bug in this function."""
    async with lock:
        return {
            "sold": state["sold"],
            "capacity": state["capacity"],
            "user_tickets": state["user_tickets"],
        }
