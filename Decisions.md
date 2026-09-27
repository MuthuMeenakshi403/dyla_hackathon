Architecture

I built three versions to show the actual progression from broken to correct, not just the final answer.

Naive version (seller/naive/main.py): a plain in-memory Python dict, no locking, no use of request_id for deduplication at all. This was deliberately built to be a plausible first draft, the kind of thing someone ships before thinking about concurrency.

First fix (seller/fixed_lock/main.py): an in-process asyncio.Lock() wrapping the entire check-and-write sequence for /buy, plus a proper idempotency cache keyed on request_id. This is correct for a single process, since the lock serializes every coroutine on one event loop, but I confirmed it fundamentally cannot extend to multiple worker processes: each uvicorn worker gets its own separate memory and its own separate lock, so running the same server with more than one worker breaks all four invariants immediately, there is nothing for the lock to coordinate across processes.

Final version (seller/fixed_sqlite/main.py): SQLite with a single atomic INSERT ... SELECT ... WHERE statement that performs the capacity check and the ticket insert as one operation, plus a UNIQUE constraint on request_id as a second, independent layer of idempotency protection. I chose SQLite over Redis or Postgres given the time budget, it needs no separate server process to set up, and its file-level write serialization gives me a single source of truth external to any one Python process, which is exactly what's needed if this were ever run across multiple workers or instances.

Rejected: a plain in-memory dict with a lock as the final answer, rejected because it silently fails the moment anyone scales beyond one process, and this is a system explicitly designed to survive real concurrent load.

Evidence

Naive run (capacity 100, 500 buyers, 50 duplicate retries):

sold=100, capacity=100 -> Invariant 1 PASSED this run
Invariant 3 FAILED: 2 request_ids each mapped to two different ticket numbers
Invariant 4 FAILED: status.sold=100 but only 98 distinct tickets were actually issued

The interesting result here isn't a raw oversell, it's something subtler and arguably worse. Because the naive version never uses request_id for deduplication, every retry is treated as a brand new purchase. In this run, two buyers' retried requests each silently consumed a second ticket, meaning two other real buyers who should have gotten a ticket were told sold out instead, even though the total count matched capacity exactly. A system that "looks correct" by total count while quietly reassigning who actually gets a ticket is a worse failure than a simple oversell, because it would pass a naive glance at /status and still be wrong. This also demonstrates that a single passing run of a race-prone system proves nothing, this exact server would show a different failure, or none, on a different run depending on timing, which is why automated invariant checking on every run matters more than eyeballing one result.

SQLite run, same parameters:

All four invariants PASSED
sold=100, capacity=100, 100 distinct tickets, 15 idempotent replays correctly returned their 
original ticket number

This confirms the fix. The honest cost: median latency went from 741ms on the naive version to 3185ms here, roughly four times slower. Correctness was not free, and I want to be upfront about that rather than let it pass silently.

Load behavior and bottleneck
Buyers	Requests/sec	Median latency	p99 latency
500	89.9	3185ms	5990ms
1000	59.3	9495ms	16594ms
5000	55.4	43317ms	89010ms

Throughput flattens hard between 1000 and 5000 buyers, settling around 55 to 60 requests per second regardless of how much additional load is thrown at it. My hypothesis, based on this flattening pattern rather than a guess: get_conn() opens a brand new SQLite connection and re-runs the WAL pragma on every single request, with no connection reuse. That per-request connection overhead is a stronger candidate for this ceiling than SQLite's write serialization itself at this scale. [If you ran the connection-reuse test: state the before/after numbers here as confirmation. If not: "I did not have time to test this directly; the concrete next step would be reusing a single connection across requests and re-running this same curve to confirm."]

What I did not get to, and why

Given the time constraint, I chose depth on the core requirements and one clear extension (the single-process to multi-process limitation, demonstrated directly rather than assumed) over breadth across several partial extensions. I did not verify the SQLite version under multiple uvicorn workers directly, though the architecture should hold given SQLite's file-level write serialization is external to any one process's memory, this needs an actual run to confirm rather than remain a claim. I also did not simulate a slow or temporarily unavailable datastore mid-sale, or build the waitlist state machine. With two more weeks, I would prioritize, in order: confirming the multi-worker claim with a real run, adding connection pooling to close the throughput ceiling, then the datastore failure-injection test, since durability under partial failure is the next most realistic production concern after raw concurrency correctness.

Testing approach

The load client (buyer/load_test.py) fires a configurable number of concurrent buy requests using asyncio and httpx, deliberately injecting duplicate request_ids sampled from the same batch and shuffled in, so genuine retries race against the originals rather than arriving safely after the fact. After the storm, it checks all four invariants directly against /status: sold count within capacity, no ticket number issued to two different request_ids, no request_id mapped to more than one ticket, and the server's own bookkeeping matching the actual distinct tickets issued. Every result in this document comes from this harness's actual output, not a manual inspection.