"""Periodic diff of every channel against the DB.

Webhooks get lost and pushes get dropped, so this is what actually guarantees convergence.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import asyncpg

from app.channel_client import ChannelClient, ChannelError
from app.ingest import ChannelEvent, apply_event, enqueue_unit_sync

log = logging.getLogger(__name__)


@dataclass
class ReconcileReport:
    listings_checked: int = 0
    listings_unreachable: int = 0
    calendars_drifted: int = 0
    missed_events_ingested: int = 0
    corrections_queued: int = 0

    @property
    def healed_anything(self) -> bool:
        return bool(self.calendars_drifted or self.missed_events_ingested or self.corrections_queued)


async def reconcile_once(pool: asyncpg.Pool, client: ChannelClient) -> ReconcileReport:
    report = ReconcileReport()
    listings = await pool.fetch(
        "SELECT id, tenant_id, unit_id, channel, external_listing_id FROM channel_listings"
    )
    for lst in listings:
        try:
            state = await client.get_listing_state(lst["channel"], lst["external_listing_id"])
        except ChannelError:
            report.listings_unreachable += 1
            continue
        report.listings_checked += 1
        await _check_bookings(pool, lst, state["bookings"], report)
        await _check_calendar(pool, lst, state["blocked"], report)
    return report


async def _check_bookings(pool, lst, bookings: list[dict], report: ReconcileReport) -> None:
    for b in bookings:
        row = await pool.fetchrow(
            "SELECT status, stay, channel_version FROM reservations WHERE channel = $1 AND external_id = $2",
            lst["channel"],
            b["reservation_id"],
        )

        if row is None or row["channel_version"] < b["version"]:
            # we never got this version's webhook
            event_type = "reservation.cancelled" if b["status"] == "cancelled" else "reservation.modified"
            await apply_event(
                pool,
                ChannelEvent(
                    channel=lst["channel"],
                    event_id=f"reconcile:{b['reservation_id']}:v{b['version']}",
                    type=event_type,
                    listing_id=lst["external_listing_id"],
                    reservation_id=b["reservation_id"],
                    version=b["version"],
                    check_in=b["check_in"],
                    check_out=b["check_out"],
                    guest=b.get("guest"),
                ),
            )
            report.missed_events_ingested += 1
            continue

        want_status = "confirmed" if row["status"] == "confirmed" else "cancelled"
        want = (want_status, row["stay"].lower.isoformat(), row["stay"].upper.isoformat())
        have = (b["status"], b["check_in"], b["check_out"])
        if have != want and row["channel_version"] == b["version"]:
            # same version but different state means our correction never landed
            log.info("re-sending correction for %s/%s", lst["channel"], b["reservation_id"])
            await pool.execute(
                """
                INSERT INTO outbox (tenant_id, listing_id, kind, payload)
                VALUES ($1, $2, 'reservation_correction', $3)
                """,
                lst["tenant_id"],
                lst["id"],
                {
                    "reservation_id": b["reservation_id"],
                    "if_version": b["version"],
                    "status": want[0],
                    "check_in": want[1],
                    "check_out": want[2],
                },
            )
            report.corrections_queued += 1


async def _check_calendar(pool, lst, channel_blocked: list[list[str]], report: ReconcileReport) -> None:
    rows = await pool.fetch(
        "SELECT lower(stay) AS s, upper(stay) AS e FROM reservations WHERE unit_id = $1 AND status = 'confirmed'",
        lst["unit_id"],
    )
    truth = set()
    for r in rows:
        truth |= _nights(r["s"], r["e"])

    theirs = set()
    for s, e in channel_blocked:
        theirs |= _nights(date.fromisoformat(s), date.fromisoformat(e))

    if theirs != truth:
        async with pool.acquire() as con:
            await enqueue_unit_sync(con, lst["unit_id"])
        report.calendars_drifted += 1


def _nights(check_in: date, check_out: date) -> set[int]:
    return set(range(check_in.toordinal(), check_out.toordinal()))
