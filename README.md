# channel-sync

Availability sync for short-term rentals listed on several booking channels at once. A night can't be sold twice, even when channels oversell, send webhooks twice or out of order, drop them, rate limit us or go down. When two channels do sell the same night, the second guest is moved to a comparable free apartment instead of being cancelled, and an operator approves what they're told.

FastAPI + Postgres (works on Supabase). Design notes and trade-offs are in [docs/DESIGN.md](docs/DESIGN.md).

## Where this fits

Channel managers like Channex or Hostaway already handle delivery: the API connections to Airbnb, Booking.com and Vrbo, listing mapping, pushing availability. What they don't decide is what happens when two of those channels sell the same night, or what to tell the guest who lost. This sits between the PMS and the channel manager. Channex delivers, this decides. The mock channels in `sim/` stand in for that layer; swapping in a real adapter changes `channel_client.py` and the webhook parser, not ingest, the outbox or the reconciler.

## How it works

- Postgres enforces no overlapping confirmed stays per unit with an exclusion constraint. Nothing in the app can get around it.
- Each webhook is applied in one transaction: dedupe on event id, version check, write, queue outbound syncs.
- When two channels sell the same night, the booking that committed second loses the apartment, not the stay. It's moved to a free unit of the same room type, or an upgrade, in the same transaction. Only if nothing comparable is free is it cancelled with a conditional correction (`if_version`) to its channel.
- Each moved guest gets a message drafted for the operator (an LLM if configured, a template otherwise). Nothing is sent until someone approves it.
- A transactional outbox pushes calendars, corrections and approved messages to channels: `SKIP LOCKED` workers, leases, backoff with jitter, a per-channel rate limit and circuit breaker. Bursts of calendar changes coalesce into one push.
- A reconciler diffs every channel against the DB and heals whatever got lost.

## Running it

Needs Docker, Python 3.11+ and Node 20+.

```
docker compose up -d --wait
pip install -r requirements-dev.txt
pytest -q
python -m sim.run_chaos --actions 3000 --seed 7
```

(or `make db test chaos`)

### Chaos run

`sim/` runs the real engine against three mock channels that misbehave on purpose: 20% duplicate webhooks, 3% dropped, up to 400ms of delay (so events arrive out of order), 8% 429s, 5% 500s, and Booking.com fully down for a few seconds mid-run. Guests book, cancel and change dates across 12 apartments (six 1-beds, four 2-beds, two 3-beds). About 6% of actions are flash crowds, where the same nights get booked on all three channels at once.

When guests stop, it waits for everything to settle and then checks:
- no overlapping confirmed stays in the DB
- no apartment held by two guests on the same night, counting moved guests where they were moved to
- every channel's bookings match the DB
- every channel's calendar matches the DB

It exits 1 if any check fails, so it runs in CI.

A typical run (seed 7, 3000 actions in 15s): about 200 clashes. Around 100 losing guests are moved to another apartment, around 75 are cancelled because nothing comparable was free (in about a dozen of those, every free unit was busy at that instant), and around 25 date changes are refused. On top of that, about 200 duplicate webhooks are absorbed, 50 dropped ones are recovered by the reconciler, there are 300 push retries and 2 breaker trips during the outage, p95 push latency is 400 to 800ms, and everything settles in under 2s. Zero double bookings and zero deadlocks. Seeds 1, 11 and 42 also pass. Exact numbers vary a little between runs because the guests run on real timers.

### Control room

```
cd dashboard && npm ci && npm run build && cd ..
python -m sim.server
```

Open http://localhost:8000 and start a run. You get a board of every apartment and night, coloured by which channel's guest holds it. Nights held by two guests flash until one of them is moved or cancelled, and hatched nights are ones where a channel's calendar is behind. There are live counters, a timeline with outages shaded, and a toggle to take any channel offline mid-run.

The side panel is the operator's inbox: one drafted message per moved guest, editable, with approve and discard. Approved messages go out through the outbox like everything else, so a channel outage delays them instead of losing them.

To have an LLM draft the messages, set `ANTHROPIC_API_KEY` before starting the server (or `GUEST_DRAFTER=llm`, with `GUEST_DRAFT_MODEL` to pick the model). Without it, drafts use a plain template. A 3000-action run moves around 200 guests, so that's around 200 short model calls.

For UI work, run `npm run dev` in `dashboard/` (port 5173, proxies `/api` to 8000).

## Layout

```
app/ingest.py        webhook -> reservations, one txn; relocation on clashes
app/guest_comms.py   drafting, operator approval, sending
app/worker.py        outbox relay
app/reconciler.py    drift detection / healing
app/resilience.py    token bucket, circuit breaker, backoff
app/api.py           /webhooks/{channel}, /inbox, /metrics
db/schema.sql
sim/mock_channel.py  fake channel with injectable failures
sim/scenario.py      chaos scenario + checks
sim/run_chaos.py     CLI
sim/server.py        control room API
dashboard/           React + TS
tests/
```

## What's next

- real Channex / Hostaway adapters
- relocation that knows about location, price and operator rules, not only room type and size
- webhook signature verification, per-tenant auth
- deploy the relay on SQS + Lambda
