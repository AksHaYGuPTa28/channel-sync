"""Outbox relay: pushes calendars, corrections and approved guest messages out to the channels.

Safe to run many of these. Rows are claimed with SKIP LOCKED and leased via locked_until, so
a crashed worker's rows get picked up again. Delivery is at-least-once, which is fine because
every payload is absolute state.
"""

from __future__ import annotations

import asyncio
import logging

import asyncpg

from app.channel_client import ChannelClient, ChannelError, VersionConflict
from app.guest_comms import mark_sent
from app.resilience import backoff_seconds

log = logging.getLogger(__name__)

MAX_ATTEMPTS = 10
LEASE_SECONDS = 30

CLAIM_SQL = f"""
UPDATE outbox o
SET status = 'inflight',
    attempts = o.attempts + 1,
    locked_until = now() + interval '{LEASE_SECONDS} seconds'
FROM (
    SELECT id FROM outbox
    WHERE (status = 'pending' AND available_at <= now())
       OR (status = 'inflight' AND locked_until < now())
    ORDER BY available_at
    FOR UPDATE SKIP LOCKED
    LIMIT $1
) claimed, channel_listings cl
WHERE o.id = claimed.id AND cl.id = o.listing_id
RETURNING o.*, cl.channel, cl.external_listing_id, cl.unit_id
"""


async def run_worker(pool: asyncpg.Pool, client: ChannelClient, stop: asyncio.Event, batch: int = 25) -> None:
    while not stop.is_set():
        rows = await pool.fetch(CLAIM_SQL, batch)
        if not rows:
            await asyncio.sleep(0.02)
            continue
        await asyncio.gather(*(process(pool, client, r) for r in rows))


async def process(pool: asyncpg.Pool, client: ChannelClient, row) -> None:
    channel = row["channel"]
    breaker = client.breakers[channel]
    if not breaker.allow():
        # channel is down, park it without using up an attempt
        await reschedule(pool, row, breaker.retry_in() + 0.05, "circuit open", refund_attempt=True)
        return

    try:
        if row["kind"] == "sync_calendar":
            await _push_calendar(pool, client, row)
        elif row["kind"] == "guest_message":
            await client.send_guest_message(channel, row["external_listing_id"], row["payload"])
        else:
            await client.correct_reservation(channel, row["external_listing_id"], row["payload"])
    except VersionConflict:
        # guest changed the booking after we decided; that change comes back through ingest
        breaker.record_success()
        await finish(pool, row["id"], "superseded")
        return
    except ChannelError as exc:
        breaker.record_failure()
        if not exc.retryable or row["attempts"] >= MAX_ATTEMPTS:
            log.error("giving up on outbox %s (%s): %s", row["id"], row["kind"], exc)
            await finish(pool, row["id"], "dead", str(exc))
        else:
            await reschedule(pool, row, backoff_seconds(row["attempts"]), str(exc))
        return

    breaker.record_success()
    await finish(pool, row["id"], "done")
    if row["kind"] == "guest_message":
        await mark_sent(pool, row["payload"]["message_id"])


async def _push_calendar(pool, client: ChannelClient, row) -> None:
    # read state at send time, not enqueue time, so a coalesced row sends the latest calendar
    blocked = await pool.fetch(
        """
        SELECT lower(stay) AS s, upper(stay) AS e FROM reservations
        WHERE unit_id = $1 AND status = 'confirmed'
        ORDER BY lower(stay)
        """,
        row["unit_id"],
    )
    await client.put_calendar(row["channel"], row["external_listing_id"], [(b["s"], b["e"]) for b in blocked])


async def finish(pool, outbox_id: int, status: str, error: str | None = None) -> None:
    await pool.execute(
        """
        UPDATE outbox
        SET status = $2, last_error = coalesce($3, last_error), processed_at = clock_timestamp(), locked_until = NULL
        WHERE id = $1
        """,
        outbox_id,
        status,
        error,
    )


async def reschedule(pool, row, delay: float, error: str, refund_attempt: bool = False) -> None:
    try:
        await pool.execute(
            """
            UPDATE outbox
            SET status = 'pending', locked_until = NULL, last_error = $3,
                available_at = now() + make_interval(secs => $2),
                attempts = attempts - CASE WHEN $4 THEN 1 ELSE 0 END
            WHERE id = $1
            """,
            row["id"],
            delay,
            error,
            refund_attempt,
        )
    except asyncpg.UniqueViolationError:
        # a newer sync for this listing is already pending and will carry fresher state
        await finish(pool, row["id"], "superseded", error)
