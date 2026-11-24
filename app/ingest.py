"""Apply channel webhooks to the reservations table.

Everything for one event happens in a single transaction: dedupe on event id, version check,
write the reservation, and queue the outbound calendar syncs. Double bookings are rejected by
the `no_double_booking` exclusion constraint, not by anything in here. When a booking loses,
we try to move the guest to a comparable unit before falling back to cancelling it.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import date
from typing import Literal

import asyncpg
from pydantic import BaseModel

log = logging.getLogger(__name__)

EventType = Literal["reservation.created", "reservation.modified", "reservation.cancelled"]
Outcome = Literal["applied", "conflict", "duplicate", "stale", "unknown_listing"]

MAX_ATTEMPTS = 6
RELOCATE_ROUNDS = 4
retry_stats = {"deadlock_retries": 0, "relocations_gave_up_busy": 0}


class ChannelEvent(BaseModel):
    channel: str
    event_id: str
    type: EventType
    listing_id: str
    reservation_id: str
    version: int  # per reservation, bumped by the channel on every change
    check_in: date
    check_out: date
    guest: str | None = None


async def apply_event(pool: asyncpg.Pool, ev: ChannelEvent) -> Outcome:
    # The per-unit lock in _apply means we shouldn't see deadlocks any more, but if one
    # does happen nothing was committed, so retrying the whole thing is safe.
    for attempt in range(MAX_ATTEMPTS):
        try:
            return await _apply_in_txn(pool, ev)
        except (asyncpg.DeadlockDetectedError, asyncpg.SerializationError):
            if attempt == MAX_ATTEMPTS - 1:
                raise
            retry_stats["deadlock_retries"] += 1
            log.warning("deadlock applying %s/%s, retrying", ev.channel, ev.event_id)
            await asyncio.sleep(random.uniform(0, 0.01 * 2**attempt))
    raise AssertionError("unreachable")


async def _apply_in_txn(pool: asyncpg.Pool, ev: ChannelEvent) -> Outcome:
    async with pool.acquire() as con, con.transaction():
        inserted = await con.fetchval(
            """
            INSERT INTO inbound_events (channel, external_event_id, payload)
            VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING true
            """,
            ev.channel,
            ev.event_id,
            ev.model_dump(mode="json"),
        )
        if not inserted:
            return "duplicate"

        outcome = await _apply(con, ev)
        await con.execute(
            "UPDATE inbound_events SET outcome = $3 WHERE channel = $1 AND external_event_id = $2",
            ev.channel,
            ev.event_id,
            outcome,
        )
        return outcome


async def _apply(con: asyncpg.Connection, ev: ChannelEvent) -> Outcome:
    listing = await con.fetchrow(
        "SELECT id, unit_id, tenant_id FROM channel_listings WHERE channel = $1 AND external_listing_id = $2",
        ev.channel,
        ev.listing_id,
    )
    if listing is None:
        log.info("webhook for unknown listing %s/%s", ev.channel, ev.listing_id)
        return "unknown_listing"

    # Serialise writers per unit. Without this, two overlapping inserts wait on each other's
    # exclusion check and deadlock, and on a busy unit that happens over and over.
    # A relocated guest sleeps in a different unit from their listing's, so lock that one.
    # unit_id never changes after insert, so reading it before taking the lock is fine.
    assigned = await con.fetchval(
        "SELECT unit_id FROM reservations WHERE channel = $1 AND external_id = $2",
        ev.channel,
        ev.reservation_id,
    )
    unit_id = assigned or listing["unit_id"]
    await _lock_unit(con, unit_id)

    existing = await con.fetchrow(
        "SELECT * FROM reservations WHERE channel = $1 AND external_id = $2 FOR UPDATE",
        ev.channel,
        ev.reservation_id,
    )
    if existing and ev.version <= existing["channel_version"]:
        return "stale"

    status = "cancelled" if ev.type == "reservation.cancelled" else "confirmed"
    stay = asyncpg.Range(ev.check_in, ev.check_out)
    outcome: Outcome = "applied"
    touched = {listing["unit_id"], unit_id}

    try:
        async with con.transaction():  # savepoint, so a constraint violation doesn't kill the outer txn
            if existing:
                res_id = existing["id"]
                await con.execute(
                    """
                    UPDATE reservations
                    SET stay = $2, status = $3, channel_version = $4, guest_name = $5, updated_at = now()
                    WHERE id = $1
                    """,
                    res_id,
                    stay,
                    status,
                    ev.version,
                    ev.guest,
                )
            else:
                res_id = await _insert_reservation(con, listing, ev, stay, status)
    except asyncpg.ExclusionViolationError:
        res_id, moved_to = await _lose(con, ev, listing, existing, stay)
        if moved_to:
            touched.add(moved_to)
        outcome = "conflict"

    await con.execute(
        """
        INSERT INTO audit_log (tenant_id, entity, entity_id, action, detail)
        VALUES ($1, 'reservation', $2, $3, $4)
        """,
        listing["tenant_id"],
        res_id,
        f"{ev.type}:{outcome}",
        {"channel": ev.channel, "event_id": ev.event_id, "version": ev.version},
    )
    for u in touched:
        await enqueue_unit_sync(con, u)
    return outcome


async def _lock_unit(con: asyncpg.Connection, unit_id, wait: bool = True) -> bool:
    key = f"unit:{unit_id}"
    if wait:
        await con.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", key)
        return True
    return await con.fetchval("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))", key)


async def _insert_reservation(con, listing, ev: ChannelEvent, stay, status: str, unit_id=None):
    return await con.fetchval(
        """
        INSERT INTO reservations
            (tenant_id, unit_id, listing_id, channel, external_id, stay, status, channel_version, guest_name)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        """,
        listing["tenant_id"],
        unit_id or listing["unit_id"],
        listing["id"],
        ev.channel,
        ev.reservation_id,
        stay,
        status,
        ev.version,
        ev.guest,
    )


async def _lose(con, ev: ChannelEvent, listing, existing, stay):
    """Someone else already holds these nights. First commit wins; this one loses.

    A new booking is moved to a comparable free unit if there is one. The guest keeps their
    booking, and an operator approves what we tell them. Only if nothing fits is it cancelled.
    Returns (reservation id, unit the guest was moved to or None).
    """
    contested = existing["unit_id"] if existing else listing["unit_id"]
    winner_id = await con.fetchval(
        """
        SELECT id FROM reservations
        WHERE unit_id = $1 AND status = 'confirmed' AND stay && $2 AND id IS DISTINCT FROM $3
        LIMIT 1
        """,
        contested,
        stay,
        existing["id"] if existing else None,
    )

    if existing is None:
        moved = await _relocate(con, listing, ev, stay)
        if moved:
            res_id, moved_to = moved
            conflict_id = await _record_conflict(
                con, listing["tenant_id"], contested, "guest_relocated", winner_id, res_id, moved_to
            )
            # drafting calls an LLM, so it happens in the background, never inside this txn
            await con.execute(
                "INSERT INTO guest_messages (tenant_id, conflict_id, reservation_id) VALUES ($1, $2, $3)",
                listing["tenant_id"],
                conflict_id,
                res_id,
            )
            return res_id, moved_to

        # nothing comparable is free: keep a record of it, then cancel it on the channel
        res_id = await _insert_reservation(con, listing, ev, stay, "rejected")
        kind = "booking_rejected"
        want_status, want_in, want_out = "cancelled", ev.check_in, ev.check_out
    else:
        # guest tried to move an existing stay onto sold nights: keep the old dates
        res_id = existing["id"]
        await con.execute(
            "UPDATE reservations SET channel_version = $2, updated_at = now() WHERE id = $1",
            res_id,
            ev.version,
        )
        kind = "modification_rejected"
        old = existing["stay"]
        want_status = "confirmed" if existing["status"] == "confirmed" else "cancelled"
        want_in, want_out = old.lower, old.upper

    await _record_conflict(con, listing["tenant_id"], contested, kind, winner_id, res_id)
    # if_version makes the correction conditional on the channel: if the guest has changed
    # the booking again since, the channel refuses it and the newer change wins.
    await con.execute(
        "INSERT INTO outbox (tenant_id, listing_id, kind, payload) VALUES ($1, $2, 'reservation_correction', $3)",
        listing["tenant_id"],
        listing["id"],
        {
            "reservation_id": ev.reservation_id,
            "if_version": ev.version,
            "status": want_status,
            "check_in": want_in.isoformat(),
            "check_out": want_out.isoformat(),
        },
    )
    return res_id, None


async def _relocate(con, listing, ev: ChannelEvent, stay):
    """Find the guest another unit from the same operator: same room type first, then
    upgrades by bedroom count. Never a downgrade."""
    # We already hold the contested unit's lock, and waiting for a second one can deadlock
    # against a transaction moving a guest the other way. So locks are only ever waited for in
    # increasing unit id order: a unit above everything we hold, we wait for; one below, we
    # only try-lock and skip if busy. Every wait points upwards, so no cycle can form.
    highest = str(listing["unit_id"])
    for attempt in range(RELOCATE_ROUNDS):
        candidates = await _relocation_candidates(con, listing["unit_id"], stay)
        if not candidates:
            return None
        for unit_id in candidates:
            if str(unit_id) > highest:
                await _lock_unit(con, unit_id)
                highest = str(unit_id)
            elif not await _lock_unit(con, unit_id, wait=False):
                continue
            try:
                async with con.transaction():
                    res_id = await _insert_reservation(con, listing, ev, stay, "confirmed", unit_id=unit_id)
            except asyncpg.ExclusionViolationError:
                continue  # taken between our check and the insert
            return res_id, unit_id
        await asyncio.sleep(random.uniform(0, 0.005 * 2**attempt))
    retry_stats["relocations_gave_up_busy"] += 1
    return None


async def _relocation_candidates(con, unit_id, stay) -> list:
    rows = await con.fetch(
        """
        SELECT u.id
        FROM units u JOIN units here ON here.id = $1
        WHERE u.tenant_id = here.tenant_id
          AND u.id <> here.id
          AND (u.room_type = here.room_type OR u.bedrooms > here.bedrooms)
          AND NOT EXISTS (
              SELECT 1 FROM reservations r
              WHERE r.unit_id = u.id AND r.status = 'confirmed' AND r.stay && $2
          )
        ORDER BY u.room_type = here.room_type DESC, u.bedrooms, u.name
        LIMIT 5
        """,
        unit_id,
        stay,
    )
    return [r["id"] for r in rows]


async def _record_conflict(con, tenant_id, unit_id, kind, winner_id, loser_id, moved_to=None) -> int:
    return await con.fetchval(
        """
        INSERT INTO conflicts
            (tenant_id, unit_id, kind, winner_reservation_id, loser_reservation_id, moved_to_unit_id)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id
        """,
        tenant_id,
        unit_id,
        kind,
        winner_id,
        loser_id,
        moved_to,
    )


async def enqueue_unit_sync(con: asyncpg.Connection, unit_id) -> None:
    """Queue a calendar push to every listing of the unit (coalesced by outbox_one_pending_sync)."""
    await con.execute(
        """
        INSERT INTO outbox (tenant_id, listing_id, kind)
        SELECT tenant_id, id, 'sync_calendar' FROM channel_listings WHERE unit_id = $1
        ON CONFLICT (listing_id) WHERE status = 'pending' AND kind = 'sync_calendar' DO NOTHING
        """,
        unit_id,
    )
