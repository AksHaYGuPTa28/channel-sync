import asyncio
from datetime import date

import pytest

from app.ingest import ChannelEvent, apply_event
from app.metrics import DOUBLE_BOOKINGS_SQL
from tests.conftest import CHANNELS

pytestmark = pytest.mark.asyncio


def ev(
    channel="airbnb",
    rid="R1",
    version=1,
    type="reservation.created",
    ci=date(2027, 5, 1),
    co=date(2027, 5, 4),
    event_id=None,
):
    return ChannelEvent(
        channel=channel,
        event_id=event_id or f"{channel}-{rid}-v{version}-{type}",
        type=type,
        listing_id=f"{channel}-L1",
        reservation_id=rid,
        version=version,
        check_in=ci,
        check_out=co,
    )


async def test_flash_crowd_has_one_winner(pool, unit):
    events = [
        ev(channel=CHANNELS[i % 3], rid=f"R{i}", ci=date(2027, 5, 1 + i % 3), co=date(2027, 5, 5))
        for i in range(60)
    ]
    outcomes = await asyncio.gather(*(apply_event(pool, e) for e in events))

    assert outcomes.count("applied") == 1
    assert outcomes.count("conflict") == 59
    assert await pool.fetchval(DOUBLE_BOOKINGS_SQL) == 0
    assert await pool.fetchval("SELECT count(*) FROM reservations WHERE status = 'rejected'") == 59
    assert await pool.fetchval("SELECT count(*) FROM outbox WHERE kind = 'reservation_correction'") == 59


async def test_without_constraint_it_double_books(pool, unit):
    # sanity check that the constraint is what's actually protecting us
    await pool.execute("ALTER TABLE reservations DROP CONSTRAINT no_double_booking")
    await asyncio.gather(*(apply_event(pool, ev(channel=CHANNELS[i % 3], rid=f"R{i}")) for i in range(30)))
    assert await pool.fetchval(DOUBLE_BOOKINGS_SQL) > 0


async def test_duplicate_webhook_applied_once(pool, unit):
    e = ev(event_id="evt-1")
    outcomes = await asyncio.gather(*(apply_event(pool, e) for _ in range(20)))
    assert outcomes.count("applied") == 1
    assert outcomes.count("duplicate") == 19
    assert await pool.fetchval("SELECT count(*) FROM reservations") == 1


async def test_cancel_arriving_before_create(pool, unit):
    assert await apply_event(pool, ev(version=2, type="reservation.cancelled")) == "applied"
    assert await apply_event(pool, ev(version=1)) == "stale"
    assert await pool.fetchval("SELECT status FROM reservations") == "cancelled"


async def test_cancel_frees_nights(pool, unit):
    await apply_event(pool, ev(rid="A"))
    assert await apply_event(pool, ev(channel="booking", rid="B")) == "conflict"
    await apply_event(pool, ev(rid="A", version=2, type="reservation.cancelled"))
    assert await apply_event(pool, ev(channel="vrbo", rid="C")) == "applied"


async def test_back_to_back_stays(pool, unit):
    assert await apply_event(pool, ev(rid="A", ci=date(2027, 5, 1), co=date(2027, 5, 4))) == "applied"
    assert (
        await apply_event(pool, ev(channel="booking", rid="B", ci=date(2027, 5, 4), co=date(2027, 5, 6)))
        == "applied"
    )


async def test_modify_onto_sold_nights_keeps_old_dates(pool, unit):
    await apply_event(pool, ev(rid="A", ci=date(2027, 5, 1), co=date(2027, 5, 3)))
    await apply_event(pool, ev(channel="booking", rid="B", ci=date(2027, 5, 10), co=date(2027, 5, 12)))

    moved = ev(
        channel="booking",
        rid="B",
        version=2,
        type="reservation.modified",
        ci=date(2027, 5, 2),
        co=date(2027, 5, 5),
    )
    assert await apply_event(pool, moved) == "conflict"

    row = await pool.fetchrow("SELECT status, lower(stay) AS s FROM reservations WHERE external_id = 'B'")
    assert row["status"] == "confirmed"
    assert row["s"] == date(2027, 5, 10)

    payload = await pool.fetchval("SELECT payload FROM outbox WHERE kind = 'reservation_correction'")
    assert payload["check_in"] == "2027-05-10"
    assert payload["if_version"] == 2


async def test_calendar_syncs_coalesce(pool, unit):
    for i in range(50):
        await apply_event(pool, ev(rid=f"R{i}", ci=date(2027, 6, 1 + i % 25), co=date(2027, 6, 2 + i % 25)))
    pending = await pool.fetchval(
        "SELECT count(*) FROM outbox WHERE kind = 'sync_calendar' AND status = 'pending'"
    )
    assert pending == len(CHANNELS)


async def test_unknown_listing(pool, unit):
    e = ev()
    e.listing_id = "nope"
    assert await apply_event(pool, e) == "unknown_listing"
    assert await pool.fetchval("SELECT count(*) FROM reservations") == 0
