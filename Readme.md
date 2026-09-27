Ticket Stampede

A ticket-selling API that guarantees four invariants under concurrent load: never oversell, never issue a duplicate ticket number, idempotent handling of repeated requests, and a /status endpoint that always matches what was actually issued.

This repo contains three versions of the seller, showing the actual progression from broken to correct, plus a load-testing client built to break it.

Requirements
Python 3.10 or later
pip
Setup (under 2 minutes)

From the project root:

pip install fastapi uvicorn httpx pydantic
Project structure
seller/
  naive/          naive version, deliberately broken, no locking, no idempotency
  fixed_lock/      first fix, asyncio.Lock, correct for a single process only
  fixed_sqlite/    final fix, atomic SQLite operations, holds across processes
buyer/
  load_test.py     async load client, fires concurrent buys, checks all four invariants
logs/              saved outputs from real runs, naive failing run, sqlite passing run, load curve
DECISIONS.md       architecture, evidence, trade-offs, what's left undone
README.md          this file
How to run

Each seller version runs as its own FastAPI app on its own port. Run one seller at a time, then attack it with the load client from a separate terminal window.

Naive version (expected to fail under load):

python -m uvicorn seller.naive.main:app --port 8001

Fixed version, single-process lock (correct with one worker, breaks with more than one):

python -m uvicorn seller.fixed_lock.main:app --port 8002

Fixed version, SQLite (the final, recommended version):

python -m uvicorn seller.fixed_sqlite.main:app --port 8000

Leave whichever server you're testing running in its own terminal window.

Running the load test

In a second, separate terminal window, from the project root:

python buyer/load_test.py --base-url http://127.0.0.1:8000 --capacity 100 --buyers 500

Arguments:

--base-url which seller instance to attack
--capacity how many tickets the sale should have, this triggers a /reset before the storm
--buyers number of unique concurrent buyers to simulate
--duplicates number of duplicate request retries to inject, default 50
--no-reset skip resetting state before firing, useful if you want to continue an existing sale

To save output instead of just printing it:

python buyer/load_test.py --base-url http://127.0.0.1:8000 --capacity 100 --buyers 500 > logs/my_run.txt
Reproducing the core result
Start the naive seller on port 8001, run the load test against it, note invariant failures.
Stop it, start the SQLite seller on port 8000, run the same load test, confirm all four invariants pass.
See logs/ for the actual saved runs referenced in DECISIONS.md.
Endpoints
Method	Path	Body	Description
POST	/reset	{"ticket_count": int}	Wipes state, starts a fresh sale
POST	/buy	{"user_id": str, "request_id": str}	Returns a ticket number or sold-out
GET	/status	—	Returns sold count and full user-to-ticket mapping
Notes
On Windows, if the bare uvicorn command isn't recognized, always use python -m uvicorn instead, as shown above.
Each seller/* folder needs an __init__.py file present for the dotted module paths above to resolve correctly.
The SQLite version creates its own .db file inside its folder on first run, safe to delete to reset state entirely between test sessions.

See DECISIONS.md for the architecture reasoning, real evidence from load runs, known limitations, and what would come next with more time.