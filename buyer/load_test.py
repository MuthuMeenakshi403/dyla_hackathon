"""
Async load-testing client for the ticket-selling API.

Fires many concurrent POST /buy requests (a mix of unique buyers and
deliberate duplicate request_ids to probe idempotency), then checks
the server's final state against four invariants and prints
throughput / latency stats plus a PASS/FAIL report.
"""

import argparse
import asyncio
import random
import statistics
import time
import uuid
from collections import defaultdict

import httpx


def parse_args():
    p = argparse.ArgumentParser(description="Load test the ticket-selling API")
    p.add_argument("--base-url", default="http://127.0.0.1:8000")
    p.add_argument("--capacity", type=int, default=100)
    p.add_argument("--buyers", type=int, default=500, help="number of unique concurrent buyers")
    p.add_argument("--duplicates", type=int, default=50,
                    help="number of duplicate/retry requests to inject, reusing existing user_id/request_id pairs")
    p.add_argument("--no-reset", action="store_true", help="skip calling /reset before the storm")
    p.add_argument("--max-inflight", type=int, default=0,
                    help="cap concurrent in-flight requests via a semaphore (0 = unbounded, fire everything at once)")
    return p.parse_args()


async def fire_buy(client, base_url, user_id, request_id, results, sem=None):
    async def _do():
        start = time.perf_counter()
        try:
            resp = await client.post(f"{base_url}/buy", json={"user_id": user_id, "request_id": request_id})
            latency = time.perf_counter() - start
            results.append({
                "user_id": user_id,
                "request_id": request_id,
                "status_code": resp.status_code,
                "body": resp.json(),
                "latency": latency,
                "error": None,
            })
        except Exception as exc:
            latency = time.perf_counter() - start
            results.append({
                "user_id": user_id,
                "request_id": request_id,
                "status_code": None,
                "body": None,
                "latency": latency,
                "error": str(exc),
            })

    if sem is not None:
        async with sem:
            await _do()
    else:
        await _do()


async def run_storm(base_url, capacity, buyers, duplicates, do_reset, max_inflight=0):
    limits = httpx.Limits(max_connections=2000, max_keepalive_connections=2000)
    async with httpx.AsyncClient(limits=limits, timeout=120.0) as client:
        if do_reset:
            r = await client.post(f"{base_url}/reset", json={"ticket_count": capacity})
            r.raise_for_status()
            print(f"Reset seller: capacity={capacity} -> {r.json()}\n")

        # Unique buyers: distinct user_id + request_id per call.
        base_calls = [(f"user-{i}", f"req-{uuid.uuid4()}") for i in range(buyers)]

        # Retry storm: replay some already-generated (user_id, request_id)
        # pairs verbatim, fired in the SAME wave, simulating a client
        # that retried a buy call it thought had failed/timed out.
        dup_calls = random.sample(base_calls, k=min(duplicates, len(base_calls)))

        all_calls = base_calls + dup_calls
        random.shuffle(all_calls)

        sem = asyncio.Semaphore(max_inflight) if max_inflight > 0 else None
        results = []
        start = time.perf_counter()
        await asyncio.gather(*[
            fire_buy(client, base_url, uid, rid, results, sem) for uid, rid in all_calls
        ])
        wall_time = time.perf_counter() - start

        status_resp = await client.get(f"{base_url}/status")
        status = status_resp.json()

        return results, wall_time, status, base_calls, dup_calls


def percentile(sorted_vals, pct):
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * pct
    f, c = int(k), min(int(k) + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def report(results, wall_time, status, capacity, base_calls, dup_calls):
    total = len(results)
    latencies = sorted(r["latency"] for r in results)
    median_ms = statistics.median(latencies) * 1000 if latencies else 0.0
    p99_ms = percentile(latencies, 0.99) * 1000
    rps = total / wall_time if wall_time > 0 else float("inf")

    successes = [r for r in results if r.get("body") and r["body"].get("status") == "success"]
    sold_out = [r for r in results if r.get("body") and r["body"].get("status") == "sold_out"]
    errors = [r for r in results if r.get("error")]

    print("=" * 60)
    print("LOAD TEST RESULTS")
    print("=" * 60)
    print(f"Total requests fired : {total} ({len(base_calls)} unique buyers + {len(dup_calls)} duplicate retries)")
    print(f"Wall time            : {wall_time:.3f}s")
    print(f"Requests/sec         : {rps:.1f}")
    print(f"Median latency       : {median_ms:.2f} ms")
    print(f"p99 latency          : {p99_ms:.2f} ms")
    print(f"Successful buys      : {len(successes)}")
    print(f"Sold-out responses   : {len(sold_out)}")
    print(f"Transport errors     : {len(errors)}")
    print()

    # Invariant 1: sold count never exceeds capacity
    sold = status["sold"]
    inv1 = sold <= capacity
    print(f"[{'PASS' if inv1 else 'FAIL'}] Invariant 1: sold count <= capacity  "
          f"(sold={sold}, capacity={capacity})")

    # Group successes by request_id first — a request_id succeeding
    # more than once is fine IF it's an idempotent replay returning the
    # SAME ticket every time. It's only a bug if the ticket differs.
    tickets_by_rid = defaultdict(set)
    successes_by_rid = defaultdict(int)
    for r in successes:
        tickets_by_rid[r["request_id"]].add(r["body"]["ticket_number"])
        successes_by_rid[r["request_id"]] += 1
    replayed_rids = {rid: n for rid, n in successes_by_rid.items() if n > 1}
    inconsistent = {rid: tix for rid, tix in tickets_by_rid.items() if len(tix) > 1}

    # A ticket number is only actually "issued" once per request_id —
    # replays of the same request_id echoing their own ticket don't
    # count as new issuances.
    distinct_tickets_by_rid = {rid: next(iter(tix)) for rid, tix in tickets_by_rid.items()}
    distinct_ticket_numbers = list(distinct_tickets_by_rid.values())

    # Invariant 2: no ticket number was handed out to two DIFFERENT
    # request_ids (a real collision/oversell, as opposed to one
    # request_id's own retry echoing its own ticket).
    ticket_to_rids = defaultdict(set)
    for rid, t in distinct_tickets_by_rid.items():
        ticket_to_rids[t].add(rid)
    colliding_tickets = {t: rids for t, rids in ticket_to_rids.items() if len(rids) > 1}
    inv2 = len(colliding_tickets) == 0
    print(f"[{'PASS' if inv2 else 'FAIL'}] Invariant 2: no duplicate ticket numbers issued  "
          f"(distinct tickets issued={len(distinct_ticket_numbers)}, "
          f"unique={len(set(distinct_ticket_numbers))}"
          + (f", colliding_tickets={dict(list(colliding_tickets.items())[:5])}" if colliding_tickets else "")
          + ")")

    # Invariant 3: each request_id maps to exactly ONE ticket number
    # across all its successful responses (true idempotency — a retry
    # never gets a different ticket than the original).
    inv3 = len(inconsistent) == 0
    print(f"[{'PASS' if inv3 else 'FAIL'}] Invariant 3: each request_id got exactly one ticket  "
          f"(distinct request_ids with a ticket={len(tickets_by_rid)}, "
          f"request_ids with inconsistent ticket numbers={len(inconsistent)}, "
          f"request_ids replayed (same ticket, informational)={len(replayed_rids)})")
    if inconsistent:
        sample = list(inconsistent.items())[:5]
        print(f"         sample offending request_ids: {sample}")

    # Invariant 4: server's own bookkeeping matches the actual distinct
    # tickets issued (not raw response count, which double-counts replays).
    mapped_count = sum(len(v) for v in status["user_tickets"].values())
    inv4 = (sold == mapped_count == len(set(distinct_ticket_numbers)))
    print(f"[{'PASS' if inv4 else 'FAIL'}] Invariant 4: status count matches actual tickets issued  "
          f"(status.sold={sold}, sum(user_tickets)={mapped_count}, "
          f"distinct tickets from successes={len(set(distinct_ticket_numbers))})")

    print()
    all_pass = inv1 and inv2 and inv3 and inv4
    print("OVERALL:", "ALL INVARIANTS PASSED" if all_pass
          else "INVARIANTS VIOLATED — naive seller is broken as expected")


def main():
    args = parse_args()
    results, wall_time, status, base_calls, dup_calls = asyncio.run(
        run_storm(args.base_url, args.capacity, args.buyers, args.duplicates, not args.no_reset, args.max_inflight)
    )
    report(results, wall_time, status, args.capacity, base_calls, dup_calls)


if __name__ == "__main__":
    main()
