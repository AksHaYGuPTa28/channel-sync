"""Apply channel webhooks to the reservations table.

Everything for one event happens in a single transaction: dedupe on event id, version check,
write the reservation, and queue the outbound calendar syncs. Double bookings are rejected by
the `no_double_booking` exclusion constraint, not by anything in here.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Literal

import asyncpg
from pydantic import BaseModel

log = logging.getLogger(__name__)

EventType = Literal["reservation.created", "reservation.modified", "reservation.cancelled"]
Outcome = Literal["applied", "conflict", "duplicate", "stale", "unknown_listing"]


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

    # serialise events for the same reservation, including the first insert where
    # there's no row to lock yet
    await con.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", f"{ev.channel}:{ev.reservation_id}"
    )

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
        res_id = await _reject(con, ev, listing, existing, stay)
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
    await enqueue_unit_sync(con, listing["unit_id"])
    return outcome


async def _insert_reservation(con, listing, ev: ChannelEvent, stay, status: str):
    return await con.fetchval(
        """
        INSERT INTO reservations
            (tenant_id, unit_id, listing_id, channel, external_id, stay, status, channel_version, guest_name)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        """,
        listing["tenant_id"],
        listing["unit_id"],
        listing["id"],
        ev.channel,
        ev.reservation_id,
        stay,
        status,
        ev.version,
        ev.guest,
    )


async def _reject(con, ev: ChannelEvent, listing, existing, stay):
    """Someone else already holds these nights. First commit wins; this one loses."""
    winner_id = await con.fetchval(
        """
        SELECT id FROM reservations
        WHERE unit_id = $1 AND status = 'confirmed' AND stay && $2 AND id IS DISTINCT FROM $3
        LIMIT 1
        """,
        listing["unit_id"],
        stay,
        existing["id"] if existing else None,
    )

    if existing is None:
        # new booking on sold nights: keep a record of it, then cancel it on the channel
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

    await con.execute(
        """
        INSERT INTO conflicts (tenant_id, unit_id, kind, winner_reservation_id, loser_reservation_id)
        VALUES ($1, $2, $3, $4, $5)
        """,
        listing["tenant_id"],
        listing["unit_id"],
        kind,
        winner_id,
        res_id,
    )
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
    return res_id


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
