# channel-sync

Availability sync for short-term rentals listed on several booking channels at once. The goal is simple: a night can't be sold twice, even when channels oversell, send webhooks twice or out of order, drop them, rate limit us or go down.

FastAPI + Postgres (works on Supabase). Design notes and trade-offs are in [docs/DESIGN.md](docs/DESIGN.md).

## How it works

- Postgres enforces no overlapping confirmed stays per unit with an exclusion constraint. Nothing in the app can get around it.
- Each webhook is applied in one transaction: dedupe on event id, version check, write, queue outbound syncs.
- When two channels sell the same night, the booking that committed second loses. It's kept as `rejected`, and a conditional correction (`if_version`) is sent to its channel.
- A transactional outbox pushes calendars to channels: `SKIP LOCKED` workers, leases, backoff with jitter, a per-channel rate limit and circuit breaker. Bursts of changes coalesce into one push.
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

`sim/` runs the real engine against three mock channels that misbehave on purpose: 20% duplicate webhooks, 3% dropped, up to 400ms of delay (so events arrive out of order), 8% 429s, 5% 500s, and Booking.com fully down for a few seconds mid-run. Guests book, cancel and change dates. About 6% of actions are flash crowds, where the same nights get booked on all three channels at once.

When guests stop, it waits for everything to settle and then checks:
- no overlapping confirmed stays in the DB
- no night held by two guests across the channels
- every channel's bookings match the DB
- every channel's calendar matches the DB

It exits 1 if any check fails, so it runs in CI.

A typical run (seed 7, 3000 actions in 15s, 12 units): about 200 conflicts resolved, 170 duplicate webhooks absorbed, 30 dropped webhooks recovered by the reconciler, 200 push retries, 3 breaker trips during the outage, p95 push latency around 700ms, settled in under a second. Zero double bookings. Seeds 1, 11 and 42 also pass.

### Control room

```
cd dashboard && npm ci && npm run build && cd ..
python -m sim.server
```

Open http://localhost:8000 and start a run. You get a board of every unit and night, coloured by which channel holds it. Nights sold to two guests flash until the correction lands, and hatched nights are ones where a channel's calendar is behind. There are live counters, a timeline with outages shaded, and a toggle to take any channel offline mid-run.

For UI work, run `npm run dev` in `dashboard/` (port 5173, proxies `/api` to 8000).

## Layout

```
app/ingest.py        webhook -> reservations, one txn
app/worker.py        outbox relay
app/reconciler.py    drift detection / healing
app/resilience.py    token bucket, circuit breaker, backoff
app/api.py           /webhooks/{channel}, /metrics
db/schema.sql
sim/mock_channel.py  fake channel with injectable failures
sim/scenario.py      chaos scenario + checks
sim/run_chaos.py     CLI
sim/server.py        control room API
dashboard/           React + TS
tests/
```

## Todo

- real Channex / Hostaway adapters
- webhook signature verification, per-tenant auth
- deploy the relay on SQS + Lambda
- LLM incident summaries for operators (see DESIGN.md)
