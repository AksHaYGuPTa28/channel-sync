# Design notes

The problem: one apartment is listed on Airbnb, Booking.com and Vrbo. Each channel takes bookings against its own copy of the calendar, and those copies lag behind ours. So two guests can and will book the same night on two channels a few hundred ms apart. On top of that, webhooks arrive late, twice, out of order or not at all, and channel APIs rate limit and go down.

We can't stop channels from overselling. What we can do is make sure our DB never accepts two stays on the same night, tell the losing channel to cancel, and get every channel's calendar back in line with ours as fast as possible.

## What has to hold

1. No two confirmed reservations on a unit overlap. Ever.
2. A webhook is applied at most once.
3. A late webhook never rolls a reservation back to an older state.
4. Every committed change eventually reaches every channel.
5. Channels converge to the DB even if webhooks or pushes get lost.
6. Tenants can't see each other's data.

(1) is enforced by Postgres, not by application code:

```sql
EXCLUDE USING gist (unit_id WITH =, stay WITH &&) WHERE (status = 'confirmed')
```

It holds no matter how many API replicas or workers are running and no matter which code path does the write. A test drops the constraint and shows the same race double books without it.

## Flow

```
channel webhook ──> POST /webhooks/{ch} ──> one txn:
                                              inbound_events (dedupe)
                                              reservations   (exclusion constraint)
                                              conflicts      (if we lost)
                                              outbox         (calendar syncs, corrections)

outbox workers ──> channel APIs        (rate limit, retries, circuit breaker)
reconciler     ──> pull channel state, diff against DB, queue fixes
```

### Inbound, one transaction per webhook

1. Insert into `inbound_events` with `ON CONFLICT DO NOTHING`. Nothing inserted means duplicate: return 200 so the channel stops retrying.
2. Take a per-unit advisory lock (see "Deadlocks" below).
3. Load the reservation `FOR UPDATE`. If the event version is <= the stored one, it's stale. Record it and stop.
4. Insert/update the reservation inside a savepoint. If the exclusion constraint fires, the other booking committed first and owns those nights:
   - new booking: store it as `rejected`, log a `conflicts` row, queue a correction that cancels it on its channel
   - date change onto sold nights: keep the old dates, queue a correction that moves it back
5. Queue a `sync_calendar` for every listing of the unit.

"Second commit loses" is the only rule I could find that's deterministic without coordinating across channels. Channels don't share a clock, and the DB commit order is the one total order we actually own.

Corrections carry `if_version`. The channel only applies one if the reservation is still at that version. Otherwise the guest has changed it since, and that newer change is already on its way back to us as a webhook.

### Outbound

- Workers claim rows with `FOR UPDATE SKIP LOCKED` plus a lease (`locked_until`). Run as many as you like. A crashed worker's rows come back after the lease expires.
- Payloads are absolute state ("these nights are blocked"), never deltas, so at-least-once delivery is fine.
- A partial unique index allows only one pending `sync_calendar` per listing, so 50 changes on a unit become one push. The worker reads the calendar when it sends, not when the row was queued.
- Per-channel token bucket and circuit breaker. While the breaker is open, rows get parked without using up an attempt.
- Exponential backoff with full jitter. After 10 attempts a row is marked `dead`.

### Reconciler

Webhooks and pushes both get lost in practice, so on an interval:
- channel has a booking version we never saw: ingest it as a synthetic event
- channel shows a different state at the same version: our correction never landed, queue it again
- channel calendar differs from ours: queue a sync

This is what actually guarantees convergence. Everything else just makes it fast.

## Deadlocks

The first version locked per reservation. Under the stress test, Postgres started reporting deadlocks: two transactions inserting overlapping stays each wait on the other's exclusion check. I added a retry, and that handled the chaos sim. But a test with 300 concurrent writers on a single unit kept deadlocking past 6 retries, and every deadlock costs about a second (`deadlock_timeout`).

Fix: `pg_advisory_xact_lock` keyed on the unit, taken before touching reservations. Contention is per unit anyway, so serialising per unit costs very little and removes the cycle completely. The retry stays as a backstop. The constraint is still the only thing that guarantees correctness; the lock is only there to keep things moving.

## Decisions

| | chose | over | why |
|---|---|---|---|
| overlap check | exclusion constraint | app lock / Redis lock / select-then-insert | locks break across replicas and on timeouts, select-then-insert races |
| delivery | transactional outbox | calling the channel inside the request | crash between commit and call loses the update; holding locks over slow HTTP |
| queue | Postgres `SKIP LOCKED` | SQS/Kafka | transactional with the write, one less thing to run. Swap later. |
| ordering | per-reservation version | timestamps | channel clocks disagree |
| payload | absolute state | deltas | idempotent and coalescable |
| tenancy | shared schema + RLS | schema per tenant | cheap to run, native on Supabase |

## At 100x

- Partition `inbound_events` and `outbox` by month and prune done rows.
- Move the outbox relay to SQS FIFO (message group = listing) with Lambda consumers. Postgres stays the source of truth.
- The constraint and advisory lock are both per unit, so contention doesn't grow with the number of units.
- Shard the reconciler by tenant and stagger it to stay inside channel rate limits.

## Not done

- Real channel adapters (Channex, Hostaway). The mocks use a simplified protocol.
- Webhook signature checks and per-tenant API auth. RLS policies exist, but the engine runs as owner.
- LLM incident summaries over `audit_log` for operators: read-only, never writes reservations.
