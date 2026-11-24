import asyncio
from datetime import date
from types import SimpleNamespace

import asyncpg
import httpx
import pytest
import pytest_asyncio

from app import guest_comms, ingest
from app.channel_client import ChannelClient
from app.guest_comms import InboxError, LlmDrafter, TemplateDrafter, approve, discard, draft_pending
from app.ingest import ChannelEvent, apply_event
from app.metrics import DOUBLE_BOOKINGS_SQL
from app.worker import CLAIM_SQL, process
from sim.mock_channel import Booking, Listing, MockChannel
from tests.conftest import CHANNELS

pytestmark = pytest.mark.asyncio

CI, CO = date(2027, 5, 1), date(2027, 5, 4)


@pytest_asyncio.fixture
async def building(pool):
    """Flat A and B are 1-beds, C is a 2-bed, S is a studio."""
    tenant = await pool.fetchval("INSERT INTO tenants (name) VALUES ('t') RETURNING id")
    units = {}
    for name, kind, bedrooms in [("A", "1-bed", 1), ("B", "1-bed", 1), ("C", "2-bed", 2), ("S", "studio", 0)]:
        units[name] = await pool.fetchval(
            "INSERT INTO units (tenant_id, name, room_type, bedrooms) VALUES ($1, $2, $3, $4) RETURNING id",
            tenant,
            f"Flat {name}",
            kind,
            bedrooms,
        )
        for ch in CHANNELS:
            await pool.execute(
                """
                INSERT INTO channel_listings (tenant_id, unit_id, channel, external_listing_id)
                VALUES ($1, $2, $3, $4)
                """,
                tenant,
                units[name],
                ch,
                f"{ch}-{name}",
            )
    return units


def ev(rid, channel="booking", flat="A", version=1, type="reservation.created", ci=CI, co=CO):
    return ChannelEvent(
        channel=channel,
        event_id=f"{channel}-{rid}-v{version}",
        type=type,
        listing_id=f"{channel}-{flat}",
        reservation_id=rid,
        version=version,
        check_in=ci,
        check_out=co,
        guest=f"Guest {rid}",
    )


async def unit_of(pool, rid):
    return await pool.fetchval("SELECT unit_id FROM reservations WHERE external_id = $1", rid)


async def test_loser_moves_to_same_type_unit(pool, building):
    assert await apply_event(pool, ev("W", channel="airbnb")) == "applied"
    assert await apply_event(pool, ev("L")) == "conflict"

    assert await pool.fetchval("SELECT status FROM reservations WHERE external_id = 'L'") == "confirmed"
    assert await unit_of(pool, "L") == building["B"]
    c = await pool.fetchrow("SELECT * FROM conflicts")
    assert (c["kind"], c["unit_id"], c["moved_to_unit_id"]) == (
        "guest_relocated",
        building["A"],
        building["B"],
    )
    # the guest keeps their booking, so nothing to correct on the channel
    assert await pool.fetchval("SELECT count(*) FROM outbox WHERE kind = 'reservation_correction'") == 0
    assert await pool.fetchval("SELECT status FROM guest_messages") == "drafting"
    # flat B's calendars have to block those nights now
    b_syncs = await pool.fetchval(
        """
        SELECT count(*) FROM outbox o JOIN channel_listings cl ON cl.id = o.listing_id
        WHERE o.kind = 'sync_calendar' AND cl.unit_id = $1
        """,
        building["B"],
    )
    assert b_syncs == 3


async def test_upgrade_when_same_type_is_full(pool, building):
    await apply_event(pool, ev("W", channel="airbnb"))
    await apply_event(pool, ev("X", channel="vrbo", flat="B"))
    assert await apply_event(pool, ev("L")) == "conflict"
    assert await unit_of(pool, "L") == building["C"]


async def test_no_downgrade_falls_back_to_cancel(pool, building):
    await apply_event(pool, ev("W", channel="airbnb"))
    await apply_event(pool, ev("X", channel="vrbo", flat="B"))
    await apply_event(pool, ev("Y", channel="vrbo", flat="C"))

    assert await apply_event(pool, ev("L")) == "conflict"
    # the studio is free, but it's smaller than what the guest booked
    assert await pool.fetchval("SELECT status FROM reservations WHERE external_id = 'L'") == "rejected"
    assert await pool.fetchval("SELECT kind FROM conflicts") == "booking_rejected"
    assert await pool.fetchval("SELECT count(*) FROM outbox WHERE kind = 'reservation_correction'") == 1
    assert await pool.fetchval("SELECT count(*) FROM guest_messages") == 0


async def test_flash_crowd_fills_comparable_units_then_cancels(pool, building):
    events = [ev(f"R{i}", channel=CHANNELS[i % 3]) for i in range(6)]
    outcomes = await asyncio.gather(*(apply_event(pool, e) for e in events))

    assert outcomes.count("applied") == 1
    assert await pool.fetchval(DOUBLE_BOOKINGS_SQL) == 0
    kinds = await pool.fetch("SELECT kind, count(*) AS n FROM conflicts GROUP BY kind")
    assert {r["kind"]: r["n"] for r in kinds} == {"guest_relocated": 2, "booking_rejected": 3}
    used = await pool.fetch("SELECT DISTINCT unit_id FROM reservations WHERE status = 'confirmed'")
    assert {r["unit_id"] for r in used} == {building["A"], building["B"], building["C"]}


async def test_crossing_moves_dont_deadlock(pool, building):
    # One transaction holds flat A and wants B, the other holds B and wants A. If both waited
    # for the second lock, Postgres would kill one of them as a deadlock.
    early, late = (date(2027, 5, 1), date(2027, 5, 4)), (date(2027, 5, 10), date(2027, 5, 14))
    await apply_event(pool, ev("WA", channel="airbnb", flat="A", ci=early[0], co=early[1]))
    await apply_event(pool, ev("WB", channel="airbnb", flat="B", ci=late[0], co=late[1]))
    for i, (ci, co) in enumerate([early, late]):  # 2-bed taken both times, so A and B are the only options
        await apply_event(pool, ev(f"WC{i}", channel="airbnb", flat="C", ci=ci, co=co))

    listings = {
        r["external_listing_id"]: r
        for r in await pool.fetch("SELECT id, unit_id, tenant_id, external_listing_id FROM channel_listings")
    }
    both_locked = asyncio.Barrier(2)

    async def lose(flat: str, ci: date, co: date):
        async with pool.acquire() as con, con.transaction():
            await ingest._lock_unit(con, building[flat])
            await both_locked.wait()
            e = ev(f"L{flat}", flat=flat, ci=ci, co=co)
            return await ingest._relocate(con, listings[f"booking-{flat}"], e, asyncpg.Range(ci, co))

    moved = await asyncio.gather(lose("A", *early), lose("B", *late))

    # the one holding the higher id backs off instead of waiting; the other gets its move
    assert sum(m is not None for m in moved) == 1
    assert await pool.fetchval(DOUBLE_BOOKINGS_SQL) == 0


async def test_relocated_guest_changing_dates_stays_in_new_unit(pool, building):
    await apply_event(pool, ev("W", channel="airbnb"))
    await apply_event(pool, ev("L"))
    later = ev("L", version=2, type="reservation.modified", ci=date(2027, 5, 2), co=date(2027, 5, 6))
    assert await apply_event(pool, later) == "applied"
    assert await unit_of(pool, "L") == building["B"]
    assert await pool.fetchval(DOUBLE_BOOKINGS_SQL) == 0


async def relocate_one(pool):
    await apply_event(pool, ev("W", channel="airbnb"))
    await apply_event(pool, ev("L"))
    assert await draft_pending(pool, TemplateDrafter()) == 1
    return await pool.fetchrow("SELECT * FROM guest_messages")


async def test_template_draft_waits_for_approval(pool, building):
    m = await relocate_one(pool)
    assert m["status"] == "awaiting_approval"
    assert m["drafted_by"] == "template"
    assert "Flat B" in m["body"] and "Guest" in m["body"]
    assert await pool.fetchval("SELECT count(*) FROM outbox WHERE kind = 'guest_message'") == 0


async def test_approve_queues_the_edited_message_once(pool, building):
    m = await relocate_one(pool)
    await approve(pool, m["id"], "Hi, we moved you to Flat B. Same dates, same price.")

    row = await pool.fetchrow("SELECT payload FROM outbox WHERE kind = 'guest_message'")
    assert row["payload"] == {
        "reservation_id": "L",
        "message_id": m["id"],
        "body": "Hi, we moved you to Flat B. Same dates, same price.",
    }
    with pytest.raises(InboxError) as err:
        await approve(pool, m["id"])
    assert err.value.status == 409


async def test_cannot_message_a_guest_who_cancelled(pool, building):
    m = await relocate_one(pool)
    await apply_event(pool, ev("L", version=2, type="reservation.cancelled"))
    with pytest.raises(InboxError) as err:
        await approve(pool, m["id"])
    assert err.value.status == 409
    await discard(pool, m["id"])
    assert await pool.fetchval("SELECT status FROM guest_messages") == "discarded"


async def test_worker_delivers_approved_message(pool, building):
    m = await relocate_one(pool)
    await approve(pool, m["id"])

    channel = MockChannel("booking", engine=httpx.AsyncClient(base_url="http://unused"))
    channel.listings["booking-A"] = Listing(bookings={"L": Booking("L", "booking-A", CI, CO)})
    client = ChannelClient(
        {"booking": httpx.AsyncClient(transport=httpx.ASGITransport(app=channel.app), base_url="http://ch")}
    )
    rows = [r for r in await pool.fetch(CLAIM_SQL, 100) if r["kind"] == "guest_message"]
    for _ in range(2):  # at-least-once: a redelivery must not message the guest twice
        await process(pool, client, rows[0])

    assert channel.listings["booking-A"].bookings["L"].messages == {m["id"]: m["body"]}
    assert channel.stats["guest_messages_delivered"] == 1
    assert await pool.fetchval("SELECT status FROM guest_messages") == "sent"


class FakeMessages:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


def fake_client(stop_reason="end_turn", text="Hi Guest, we've moved you to Flat B."):
    messages = FakeMessages(
        SimpleNamespace(
            stop_reason=stop_reason,
            model=guest_comms.DRAFT_MODEL,
            content=[SimpleNamespace(type="text", text=text)],
        )
    )
    return SimpleNamespace(beta=SimpleNamespace(messages=messages)), messages


async def test_llm_drafter_sends_only_the_facts(pool, building):
    client, messages = fake_client()
    await apply_event(pool, ev("W", channel="airbnb"))
    await apply_event(pool, ev("L"))
    await draft_pending(pool, LlmDrafter(client=client))

    m = await pool.fetchrow("SELECT * FROM guest_messages")
    assert (m["status"], m["drafted_by"]) == ("awaiting_approval", guest_comms.DRAFT_MODEL)
    assert m["body"] == "Hi Guest, we've moved you to Flat B."

    call = messages.calls[0]
    assert call["model"] == guest_comms.DRAFT_MODEL
    assert call["fallbacks"] == "default"
    prompt = call["messages"][0]["content"]
    assert "<facts>" in prompt and "Flat A (1 bedroom)" in prompt and "Flat B (1 bedroom)" in prompt


async def test_refused_draft_falls_back_to_template(pool, building):
    client, _ = fake_client(stop_reason="refusal", text="")
    await apply_event(pool, ev("W", channel="airbnb"))
    await apply_event(pool, ev("L"))
    await draft_pending(pool, LlmDrafter(client=client))

    m = await pool.fetchrow("SELECT * FROM guest_messages")
    assert m["status"] == "awaiting_approval"
    assert m["drafted_by"] == "template (model unavailable)"
    assert "Flat B" in m["body"]
