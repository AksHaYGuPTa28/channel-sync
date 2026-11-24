"""Chaos scenario shared by the CLI (run_chaos) and the control room (server).

Runs the real engine against three mock channels, throws guest traffic at them, lets things
settle and then checks that nothing got double booked anywhere. Nobody approves guest messages
during a run; the control room's inbox is for that.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import asdict, dataclass
from datetime import date, timedelta

import httpx

from app import metrics
from app.api import create_app
from app.channel_client import ChannelClient
from app.db import create_pool, reset_schema
from app.guest_comms import Drafter, TemplateDrafter, inbox, run_drafter
from app.reconciler import ReconcileReport, reconcile_once
from app.worker import run_worker
from sim.mock_channel import Chaos, Listing, MockChannel, nights

CHANNELS = ["airbnb", "booking", "vrbo"]
BASE_DATE = date(2027, 3, 1)
HORIZON_DAYS = 45
BOARD_DAYS = HORIZON_DAYS + 5  # stays can run past the last check-in date


def room_type(unit: int) -> tuple[str, int]:
    """12 units: six 1-beds, four 2-beds, two 3-beds."""
    if unit < 6:
        return "1-bed", 1
    if unit < 10:
        return "2-bed", 2
    return "3-bed", 3


@dataclass
class Params:
    actions: int = 3000
    units: int = 12
    workers: int = 4
    duration: float = 15.0
    seed: int = 7
    scheduled_outage: bool = True


def stormy() -> Chaos:
    return Chaos(
        p_429=0.08,
        p_500=0.05,
        latency_ms=(5, 60),
        p_dup_webhook=0.2,
        p_drop_webhook=0.03,
        webhook_delay_ms=(0, 400),
    )


def calm() -> Chaos:
    return Chaos(p_dup_webhook=0.2, webhook_delay_ms=(0, 50))


def listing_id(channel: str, unit: int) -> str:
    return f"{channel}-L{unit:03d}"


class ChaosRun:
    def __init__(self, params: Params, drafter: Drafter | None = None):
        self.p = params
        self.drafter = drafter or TemplateDrafter()
        self.rng = random.Random(params.seed)
        self.phase = "starting"
        self.started = time.monotonic()
        self.workload_s: float | None = None
        self.converge_s: float | None = None
        self.rounds = 0
        self.heals: list[ReconcileReport] = []
        self.actions = {"book": 0, "cancel": 0, "modify": 0, "flash_crowds": 0}
        self.timeline: list[dict] = []
        self.checks: dict | None = None
        self.pool = None
        self.chans: dict[str, MockChannel] = {}
        self.client: ChannelClient | None = None
        self.unit_index: dict = {}  # unit uuid -> number used in listing ids
        self.moved: dict[tuple[str, str], int] = {}  # (channel, reservation) -> unit, if relocated
        self._stop = asyncio.Event()  # guests, sampling, reconciler
        self._shutdown = asyncio.Event()  # workers and drafter run until close()
        self._tasks: list[asyncio.Task] = []

    async def run(self) -> dict:
        await reset_schema()
        self.pool = await create_pool(max_size=60)

        engine = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(self.pool)), base_url="http://engine"
        )
        for i, name in enumerate(CHANNELS):
            self.chans[name] = MockChannel(name, engine, stormy(), seed=self.p.seed + i)
        self.client = ChannelClient(
            {
                name: httpx.AsyncClient(transport=httpx.ASGITransport(app=ch.app), base_url=f"http://{name}")
                for name, ch in self.chans.items()
            }
        )
        await self._seed()

        # workers outlive the run, so messages the operator approves afterwards still go out
        self._tasks = [
            *(
                asyncio.create_task(run_worker(self.pool, self.client, self._shutdown))
                for _ in range(self.p.workers)
            ),
            asyncio.create_task(run_drafter(self.pool, self.drafter, self._shutdown)),
        ]
        background = [asyncio.create_task(self._reconcile_loop()), asyncio.create_task(self._sample_loop())]

        self.phase = "storm"
        t0 = time.monotonic()
        jobs = [self._guest_action(i) for i in range(self.p.actions)]
        if self.p.scheduled_outage:
            jobs.append(self._scheduled_outage())
        await asyncio.gather(*jobs)
        self.workload_s = time.monotonic() - t0

        self.phase = "converging"
        t1 = time.monotonic()
        await self._converge()
        self.converge_s = time.monotonic() - t1

        self._stop.set()
        await asyncio.gather(*background, return_exceptions=True)
        await self._wait_drafted()
        await self._sample()
        self.checks = await self.verify()
        self.phase = "verified"
        return await self.report()

    async def close(self) -> None:
        self._stop.set()
        self._shutdown.set()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.pool is not None:
            await self.pool.close()

    async def _seed(self) -> None:
        tenant = await self.pool.fetchval("INSERT INTO tenants (name) VALUES ('Demo Operator') RETURNING id")
        for u in range(self.p.units):
            kind, bedrooms = room_type(u)
            unit_id = await self.pool.fetchval(
                "INSERT INTO units (tenant_id, name, room_type, bedrooms) VALUES ($1, $2, $3, $4) RETURNING id",
                tenant,
                f"Unit {u + 1:02d}",
                kind,
                bedrooms,
            )
            self.unit_index[unit_id] = u
            for name, ch in self.chans.items():
                ext = listing_id(name, u)
                await self.pool.execute(
                    """
                    INSERT INTO channel_listings (tenant_id, unit_id, channel, external_listing_id)
                    VALUES ($1, $2, $3, $4)
                    """,
                    tenant,
                    unit_id,
                    name,
                    ext,
                )
                ch.listings[ext] = Listing()

    async def _converge(self) -> None:
        # outage is over and error rates drop back; drain and reconcile until nothing changes
        for ch in self.chans.values():
            ch.chaos = calm()
        for self.rounds in range(1, 40):
            await self._quiesce_channels()
            await self._wait_outbox_drained()
            rep = await reconcile_once(self.pool, self.client)
            self.heals.append(rep)
            if rep.healed_anything or rep.listings_unreachable:
                continue
            await self._quiesce_channels()
            if await self._outbox_open() == 0:
                return

    def _random_stay(self) -> tuple[date, date]:
        check_in = BASE_DATE + timedelta(days=self.rng.randrange(HORIZON_DAYS))
        return check_in, check_in + timedelta(days=self.rng.randint(1, 5))

    async def _guest_action(self, i: int) -> None:
        await asyncio.sleep(self.rng.uniform(0, self.p.duration))
        roll = self.rng.random()
        name = self.rng.choice(CHANNELS)
        ch = self.chans[name]
        held = [
            (ext, rid)
            for ext, lst in ch.listings.items()
            for rid, b in lst.bookings.items()
            if b.status == "confirmed"
        ]

        if roll < 0.06:
            # same unit + nights hit on every channel at once
            unit = self.rng.randrange(self.p.units)
            check_in, check_out = self._random_stay()
            self.actions["flash_crowds"] += 1
            for n in CHANNELS:
                self.chans[n].book(listing_id(n, unit), check_in, check_out, f"guest{i}-{n}")
        elif roll < 0.78 or not held:
            unit = self.rng.randrange(self.p.units)
            check_in, check_out = self._random_stay()
            self.actions["book"] += 1
            ch.book(listing_id(name, unit), check_in, check_out, f"guest{i}")
        elif roll < 0.89:
            ext, rid = self.rng.choice(held)
            self.actions["cancel"] += ch.cancel(ext, rid)
        else:
            ext, rid = self.rng.choice(held)
            self.actions["modify"] += ch.modify(ext, rid, *self._random_stay())

    async def _scheduled_outage(self) -> None:
        await asyncio.sleep(self.p.duration * 0.3)
        self.set_outage("booking", True)
        await asyncio.sleep(self.p.duration * 0.25)
        self.set_outage("booking", False)

    def set_outage(self, channel: str, down: bool) -> None:
        self.chans[channel].chaos.outage = down

    async def _reconcile_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(1.0)
            self.heals.append(await reconcile_once(self.pool, self.client))

    async def _sample_loop(self) -> None:
        while not self._stop.is_set():
            await self._sample()
            await asyncio.sleep(0.5)

    async def _quiesce_channels(self) -> None:
        await asyncio.gather(*(ch.quiesce() for ch in self.chans.values()))

    async def _outbox_open(self) -> int:
        return await self.pool.fetchval("SELECT count(*) FROM outbox WHERE status IN ('pending', 'inflight')")

    async def _wait_drafted(self, max_wait: float = 30) -> None:
        # instant with the template drafter; a model takes a few seconds per batch
        deadline = time.monotonic() + max_wait
        while time.monotonic() < deadline and await self.pool.fetchval(  # noqa: ASYNC110 (polling the db)
            "SELECT count(*) FROM guest_messages WHERE status = 'drafting'"
        ):
            await asyncio.sleep(0.1)

    async def _wait_outbox_drained(self, max_wait: float = 30) -> None:
        deadline = time.monotonic() + max_wait
        while await self._outbox_open() and time.monotonic() < deadline:  # noqa: ASYNC110 (polling the db)
            await asyncio.sleep(0.05)

    async def _refresh_moved(self) -> None:
        rows = await self.pool.fetch(
            """
            SELECT r.channel, r.external_id, r.unit_id
            FROM reservations r JOIN channel_listings cl ON cl.id = r.listing_id
            WHERE r.unit_id <> cl.unit_id
            """
        )
        self.moved = {(r["channel"], r["external_id"]): self.unit_index[r["unit_id"]] for r in rows}

    async def _sample(self) -> None:
        await self._refresh_moved()
        row = await self.pool.fetchrow(
            """
            SELECT
                (SELECT count(*) FROM inbound_events) AS events,
                (SELECT count(*) FROM conflicts) AS conflicts,
                (SELECT count(*) FROM outbox WHERE status IN ('pending', 'inflight')) AS queue
            """
        )
        self.timeline.append(
            {
                "t": round(time.monotonic() - self.started, 2),
                "events": row["events"],
                "conflicts": row["conflicts"],
                "queue": row["queue"],
                "db_double": await self.pool.fetchval(metrics.DOUBLE_BOOKINGS_SQL),
                "oversold_nights": self._oversold_nights(),
                "down": [n for n, ch in self.chans.items() if ch.chaos.outage],
            }
        )

    def _occupancy(self) -> dict[int, dict[int, int]]:
        """unit -> night -> number of guests sleeping there, going by the channels' bookings.

        A relocated guest still holds a booking on their original listing but sleeps in the unit
        we moved them to, so they're counted there.
        """
        occ: dict[int, dict[int, int]] = {u: {} for u in range(self.p.units)}
        for name, ch in self.chans.items():
            for ext, lst in ch.listings.items():
                listed_unit = int(ext.rsplit("-L", 1)[1])
                for b in lst.bookings.values():
                    if b.status != "confirmed":
                        continue
                    counts = occ[self.moved.get((name, b.reservation_id), listed_unit)]
                    for night in nights(b.check_in, b.check_out):
                        counts[night] = counts.get(night, 0) + 1
        return occ

    def _oversold_nights(self) -> int:
        return sum(1 for counts in self._occupancy().values() for c in counts.values() if c > 1)

    async def board(self) -> list[dict]:
        units = await self.pool.fetch("SELECT id, name, room_type FROM units ORDER BY name")
        rows = await self.pool.fetch(
            "SELECT unit_id, channel, lower(stay) s, upper(stay) e FROM reservations WHERE status = 'confirmed'"
        )
        owner: dict = {}
        for r in rows:
            for night in nights(r["s"], r["e"]):
                owner[(r["unit_id"], night)] = r["channel"]

        occupancy = self._occupancy()
        board = []
        for u, unit in enumerate(units):
            truth = {n for (uid, n) in owner if uid == unit["id"]}
            held = occupancy[u]
            stale: dict[int, list[str]] = {}
            for name, ch in self.chans.items():
                for night in ch.listings[listing_id(name, u)].blocked ^ truth:
                    stale.setdefault(night, []).append(name)

            cells = []
            for d in range(BOARD_DAYS):
                night = (BASE_DATE + timedelta(days=d)).toordinal()
                cells.append([owner.get((unit["id"], night)), held.get(night, 0) > 1, stale.get(night, [])])
            board.append({"unit": unit["name"], "room_type": unit["room_type"], "cells": cells})
        return board

    async def recent_conflicts(self, limit: int = 14) -> list[dict]:
        rows = await self.pool.fetch(
            """
            SELECT c.created_at, c.kind, u.name AS unit, m.name AS moved_to, w.channel AS winner,
                   l.channel AS loser, lower(l.stay) AS s, upper(l.stay) AS e
            FROM conflicts c
            JOIN units u ON u.id = c.unit_id
            LEFT JOIN units m ON m.id = c.moved_to_unit_id
            JOIN reservations l ON l.id = c.loser_reservation_id
            LEFT JOIN reservations w ON w.id = c.winner_reservation_id
            ORDER BY c.id DESC
            LIMIT $1
            """,
            limit,
        )
        return [
            {
                "at": r["created_at"].isoformat(),
                "kind": r["kind"],
                "unit": r["unit"],
                "moved_to": r["moved_to"],
                "winner": r["winner"],
                "loser": r["loser"],
                "check_in": r["s"].isoformat(),
                "check_out": r["e"].isoformat(),
            }
            for r in rows
        ]

    async def snapshot(self) -> dict:
        if self.pool is None or not self.chans or not self.timeline:
            return {"phase": self.phase, "params": asdict(self.p)}

        channels = {}
        for name, ch in self.chans.items():
            breaker = self.client.breakers[name]
            channels[name] = {
                **ch.stats,
                "down": ch.chaos.outage,
                "inflight_webhooks": ch.inflight_webhooks,
                "breaker": breaker.state,
                "breaker_opens": breaker.times_opened,
            }
        return {
            "phase": self.phase,
            "params": asdict(self.p),
            "elapsed": round(time.monotonic() - self.started, 1),
            "engine": await metrics.snapshot(self.pool),
            "channels": channels,
            "healed": self._healed(),
            "timeline": self.timeline[-240:],
            "board": await self.board(),
            "conflicts": await self.recent_conflicts(),
            "inbox": await inbox(self.pool, limit=8),
            "checks": self.checks,
            "base_date": BASE_DATE.isoformat(),
        }

    def _healed(self) -> dict:
        return {
            "missed_events_ingested": sum(r.missed_events_ingested for r in self.heals),
            "calendars_resynced": sum(r.calendars_drifted for r in self.heals),
            "corrections_requeued": sum(r.corrections_queued for r in self.heals),
        }

    async def report(self) -> dict:
        return {
            "params": asdict(self.p),
            "workload_seconds": round(self.workload_s or 0, 2),
            "convergence_seconds": round(self.converge_s or 0, 2),
            "convergence_rounds": self.rounds,
            "healed_by_reconciler": self._healed(),
            "guest_actions": self.actions,
            "channels": {n: ch.stats for n, ch in self.chans.items()},
            "circuit_breaker_opens": {n: self.client.breakers[n].times_opened for n in CHANNELS},
            "engine": await metrics.snapshot(self.pool),
            "checks": self.checks,
        }

    async def verify(self) -> dict:
        await self._refresh_moved()
        db_double = await self.pool.fetchval(metrics.DOUBLE_BOOKINGS_SQL)
        rows = await self.pool.fetch(
            """
            SELECT channel, external_id, lower(stay) s, upper(stay) e, unit_id
            FROM reservations
            WHERE status = 'confirmed'
            """
        )
        db_set = {(r["channel"], r["external_id"], r["s"], r["e"]) for r in rows}
        unit_of = {
            (r["channel"], r["external_listing_id"]): r["unit_id"]
            for r in await self.pool.fetch(
                "SELECT channel, external_listing_id, unit_id FROM channel_listings"
            )
        }

        channel_set = set()
        for name, ch in self.chans.items():
            for lst in ch.listings.values():
                for b in lst.bookings.values():
                    if b.status == "confirmed":
                        channel_set.add((name, b.reservation_id, b.check_in, b.check_out))

        truth: dict = {}
        for r in rows:
            truth.setdefault(r["unit_id"], set()).update(nights(r["s"], r["e"]))
        calendar_mismatches = sum(
            1
            for name, ch in self.chans.items()
            for ext, lst in ch.listings.items()
            if lst.blocked != truth.get(unit_of[(name, ext)], set())
        )
        oversold = self._oversold_nights()

        return {
            "no_double_bookings_in_db": {"ok": db_double == 0, "value": db_double},
            "no_double_bookings_on_channels": {"ok": oversold == 0, "value": oversold},
            "channels_agree_with_db": {
                "ok": channel_set == db_set,
                "only_on_channels": len(channel_set - db_set),
                "only_in_db": len(db_set - channel_set),
                "confirmed": len(db_set),
            },
            "calendars_match_db": {
                "ok": calendar_mismatches == 0,
                "mismatched_listings": calendar_mismatches,
            },
        }
