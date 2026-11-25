# Design notes

The problem: one apartment is listed on Airbnb, Booking.com and Vrbo. Each channel takes bookings against its own copy of the calendar, and those copies lag behind ours. So two guests can and will book the same night on two channels a few hundred ms apart. On top of that, webhooks arrive late, twice, out of order or not at all, and channel APIs rate limit and go down.

We can't stop channels from overselling. What we can do is make sure our DB never accepts two stays on the same night, find the losing guest somewhere else to stay, and get every channel's calendar back in line with ours as fast as possible.

Cancelling the loser is the easy answer, and it's the wrong default for an operator: a cancelled guest is a refund, a bad review, and on Airbnb a host penalty. Most operators with more than a handful of units would rather move the guest to an equivalent apartment and tell them nicely. So that's what this does, and cancelling is the fallback.

## Where this fits

Channex, Hostaway and similar channel managers own the pipes: the connections to each OTA, listing mapping, pushing rates and availability, delivering booking webhooks. They don't decide what to do when two of those channels sell the same night. This sits between the PMS and the channel manager. Channex delivers, this decides. In this repo the mock channels play the channel manager's part, and a real adapter would replace `channel_client.py` and the webhook parser without touching ingest, the outbox or the reconciler.

## What has to hold

1. No two confirmed reservations on a unit overlap. Ever.
2. A webhook is applied at most once.
3. A late webhook never rolls a reservation back to an older state.
4. Every committed change eventually reaches every channel.
5. Channels converge to the DB even if webhooks or pushes get lost.
6. Tenants can't see each other's data.
7. Nothing is said to a guest without a person approving it.

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
                                              guest_messages (if we moved the guest)
                                              outbox         (calendar syncs, corrections)

drafter        ──> LLM or template, parks drafts for approval
operator       ──> approve / edit / discard, approved messages go into the outbox
outbox workers ──> channel APIs        (rate limit, retries, circuit breaker)
reconciler     ──> pull channel state, diff against DB, queue fixes
```

### Inbound, one transaction per webhook

1. Insert into `inbound_events` with `ON CONFLICT DO NOTHING`. Nothing inserted means duplicate: return 200 so the channel stops retrying.
2. Take a per-unit advisory lock (see "Deadlocks" below). It's the unit the guest actually sleeps in, which for a moved guest isn't their listing's unit.
3. Load the reservation `FOR UPDATE`. If the event version is <= the stored one, it's stale. Record it and stop.
4. Insert/update the reservation inside a savepoint. If the exclusion constraint fires, the other booking committed first and owns those nights:
   - new booking: move the guest to a comparable free unit (next section) and queue a message draft. If nothing fits, store it as `rejected` and queue a correction that cancels it on its channel.
   - date change onto sold nights: keep the old dates, queue a correction that moves it back
5. Queue a `sync_calendar` for every listing of every unit that changed.

"Second commit loses" is the only rule I could find that's deterministic without coordinating across channels. Channels don't share a clock, and the DB commit order is the one total order we actually own.

Corrections carry `if_version`. The channel only applies one if the reservation is still at that version. Otherwise the guest has changed it since, and that newer change is already on its way back to us as a webhook.

### Moving the losing guest

A reservation has a `unit_id` (where the guest sleeps) separate from its `listing_id` (where they booked). Normally they point at the same unit. Moving a guest means inserting their reservation against another unit. Their booking on the channel doesn't change at all: same listing, dates and price. That's deliberate. OTAs don't let a host move a booking to a different listing through the API, and the guest doesn't need them to.

Which unit:
- same operator, free for every night of the stay
- same room type first, then units with more bedrooms, fewest first. Never fewer bedrooms than they booked.
- ties broken by name, so the choice is deterministic

The candidate check is a plain `NOT EXISTS`. The insert still goes through the exclusion constraint in its own savepoint, so if someone takes the unit between the check and the insert, we try the next one.

Locking is the subtle part. We already hold the contested unit's advisory lock. Waiting for a second unit's lock can deadlock against a transaction doing the reverse move. So candidates are locked with `pg_try_advisory_xact_lock`: if someone is writing to that unit right now, skip it. Worst case we cancel a guest we could have moved, which is the old behaviour. Never a deadlock.

Both units' calendars get a sync: the new unit now blocks those nights on every channel.

This is the same idea as selling a room type instead of a specific room, which is where a real PMS would take it. An operator with five identical units should sell "1-bed, Shoreditch" and assign a unit as late as possible, at which point most clashes disappear before they happen. Relocation is what you need anyway for the clashes that remain, and for operators who list units individually.

### Telling the guest

Ingest only inserts a `guest_messages` row with status `drafting`. It never calls a model: that's slow, can fail, and would hold the unit lock while it waits.

A drafter loop leases those rows (same lease pattern as the outbox), writes the text and parks it as `awaiting_approval`. With `ANTHROPIC_API_KEY` set it uses an LLM (Anthropic's API), otherwise a template. The model gets a short list of facts and nothing else: first name, channel, dates, old and new unit with bedroom counts. The guest name comes from the channel, so it's untrusted. The facts are wrapped in a tag the system prompt says is data, and nothing the model returns is acted on automatically anyway. The prompt rules out offering refunds or credit. That's the operator's decision, not the model's.

If the model call fails, is refused, or returns something empty or too long, the draft falls back to the template and says so. A person reads it either way.

The operator edits, approves or discards. Approving checks the reservation is still confirmed (the guest may have cancelled since), then writes a `guest_message` row to the outbox in the same transaction. The message id is the idempotency key on the channel side, because the outbox delivers at least once.

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
| losing guest | move to a comparable unit | cancel | a cancellation costs a refund, a review and on Airbnb a host penalty |
| second unit lock | try-lock, skip if busy | wait | waiting on two locks can deadlock; skipping only costs a missed move |
| guest message | model drafts, person approves | auto-send | wrong words to a guest cost more than a slower reply |

## At 100x

- Partition `inbound_events` and `outbox` by month and prune done rows.
- Move the outbox relay to SQS FIFO (message group = listing) with Lambda consumers. Postgres stays the source of truth.
- The constraint and advisory lock are both per unit, so contention doesn't grow with the number of units.
- Shard the reconciler by tenant and stagger it to stay inside channel rate limits.

## Not done

- Real channel adapters (Channex, Hostaway). The mocks use a simplified protocol.
- Relocation only looks at room type and bedrooms. A real one needs location (a different building may be a worse outcome than a cancellation), price difference and per-operator rules, and an undo in the inbox.
- A moved guest's original listing still shows their booking on that channel, so that channel won't resell those nights on the original listing even after they free up elsewhere. Correct, slightly conservative.
- Webhook signature checks and per-tenant API auth. RLS policies exist, but the engine runs as owner.
