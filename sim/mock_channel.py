"""Fake booking channel for the sim.

Guests book against the channel's own copy of availability, which lags behind ours. That lag
is what produces real cross-channel double sells. On top of that it can duplicate, delay and
drop webhooks, and return 429/500/503 on the calls we make to it.
"""

from __future__ import annotations

import asyncio
import itertools
import random
from dataclasses import dataclass, field
from datetime import date

import httpx
from fastapi import FastAPI, Response


@dataclass
class Chaos:
    p_429: float = 0.0
    p_500: float = 0.0
    latency_ms: tuple[int, int] = (0, 0)
    outage: bool = False
    p_dup_webhook: float = 0.0
    p_drop_webhook: float = 0.0
    webhook_delay_ms: tuple[int, int] = (0, 0)


@dataclass
class Booking:
    reservation_id: str
    listing: str
    check_in: date
    check_out: date
    status: str = "confirmed"
    version: int = 1
    guest: str = ""


@dataclass
class Listing:
    blocked: set[int] = field(default_factory=set)  # date ordinals, as last pushed by us
    bookings: dict[str, Booking] = field(default_factory=dict)


def nights(check_in: date, check_out: date) -> set[int]:
    return set(range(check_in.toordinal(), check_out.toordinal()))


def to_ranges(ordinals: set[int]) -> list[list[str]]:
    out: list[list[int]] = []
    for n in sorted(ordinals):
        if out and out[-1][1] == n:
            out[-1][1] = n + 1
        else:
            out.append([n, n + 1])
    return [[date.fromordinal(s).isoformat(), date.fromordinal(e).isoformat()] for s, e in out]


class MockChannel:
    _ids = itertools.count(1)

    def __init__(self, name: str, engine: httpx.AsyncClient, chaos: Chaos | None = None, seed: int = 0):
        self.name = name
        self.engine = engine
        self.chaos = chaos or Chaos()
        self.rng = random.Random(seed)
        self.listings: dict[str, Listing] = {}
        self.inflight_webhooks = 0
        self.stats = {
            "bookings_accepted": 0,
            "bookings_refused": 0,
            "webhooks_sent": 0,
            "webhooks_duplicated": 0,
            "webhooks_dropped": 0,
            "api_errors_injected": 0,
            "corrections_applied": 0,
        }
        self._event_seq = itertools.count(1)
        self._tasks: set[asyncio.Task] = set()
        self.app = self._build_app()

    # guest side

    def _is_free(self, listing: str, check_in: date, check_out: date, ignore: str | None = None) -> bool:
        lst = self.listings[listing]
        taken = set()
        for b in lst.bookings.values():
            if b.status == "confirmed" and b.reservation_id != ignore:
                taken |= nights(b.check_in, b.check_out)

        blocked = lst.blocked
        if ignore:
            # our pushed calendar includes the guest's own stay, don't count it against them
            own = lst.bookings[ignore]
            blocked = blocked - nights(own.check_in, own.check_out)
        return not (nights(check_in, check_out) & (taken | blocked))

    def book(self, listing: str, check_in: date, check_out: date, guest: str) -> str | None:
        if not self._is_free(listing, check_in, check_out):
            self.stats["bookings_refused"] += 1
            return None
        rid = f"{self.name[:3].upper()}-{next(self._ids):06d}"
        booking = Booking(rid, listing, check_in, check_out, guest=guest)
        self.listings[listing].bookings[rid] = booking
        self.stats["bookings_accepted"] += 1
        self._emit(booking, "reservation.created")
        return rid

    def cancel(self, listing: str, rid: str) -> bool:
        b = self.listings[listing].bookings.get(rid)
        if not b or b.status != "confirmed":
            return False
        b.status = "cancelled"
        b.version += 1
        self._emit(b, "reservation.cancelled")
        return True

    def modify(self, listing: str, rid: str, check_in: date, check_out: date) -> bool:
        b = self.listings[listing].bookings.get(rid)
        if not b or b.status != "confirmed" or not self._is_free(listing, check_in, check_out, ignore=rid):
            return False
        b.check_in, b.check_out = check_in, check_out
        b.version += 1
        self._emit(b, "reservation.modified")
        return True

    # webhooks out to the engine

    def _emit(self, b: Booking, kind: str) -> None:
        if self.rng.random() < self.chaos.p_drop_webhook:
            self.stats["webhooks_dropped"] += 1
            return
        body = {
            "event_id": f"{self.name}-evt-{next(self._event_seq)}",
            "type": kind,
            "listing_id": b.listing,
            "reservation_id": b.reservation_id,
            "version": b.version,
            "check_in": b.check_in.isoformat(),
            "check_out": b.check_out.isoformat(),
            "guest": b.guest,
        }
        copies = 2 if self.rng.random() < self.chaos.p_dup_webhook else 1
        self.stats["webhooks_duplicated"] += copies - 1
        for _ in range(copies):
            task = asyncio.create_task(self._deliver(body))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _deliver(self, body: dict) -> None:
        self.inflight_webhooks += 1
        try:
            await asyncio.sleep(self.rng.uniform(*self.chaos.webhook_delay_ms) / 1000)
            for attempt in range(8):
                try:
                    r = await self.engine.post(f"/webhooks/{self.name}", json=body, timeout=10)
                    if r.status_code < 300:
                        self.stats["webhooks_sent"] += 1
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.05 * 2**attempt)
        finally:
            self.inflight_webhooks -= 1

    async def quiesce(self) -> None:
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # API the engine calls

    async def _maybe_fail(self) -> Response | None:
        lo, hi = self.chaos.latency_ms
        if hi:
            await asyncio.sleep(self.rng.uniform(lo, hi) / 1000)
        if self.chaos.outage:
            self.stats["api_errors_injected"] += 1
            return Response(status_code=503)
        roll = self.rng.random()
        if roll < self.chaos.p_429:
            self.stats["api_errors_injected"] += 1
            return Response(status_code=429)
        if roll < self.chaos.p_429 + self.chaos.p_500:
            self.stats["api_errors_injected"] += 1
            return Response(status_code=500)
        return None

    def _build_app(self) -> FastAPI:
        app = FastAPI()

        @app.put("/listings/{listing}/calendar")
        async def put_calendar(listing: str, body: dict):
            if err := await self._maybe_fail():
                return err
            blocked = set()
            for s, e in body["blocked"]:
                blocked |= nights(date.fromisoformat(s), date.fromisoformat(e))
            self.listings[listing].blocked = blocked
            return {"ok": True}

        @app.post("/listings/{listing}/reservations/{rid}/correct")
        async def correct(listing: str, rid: str, body: dict):
            if err := await self._maybe_fail():
                return err
            b = self.listings[listing].bookings[rid]
            if b.version != body["if_version"]:
                return Response(status_code=409, content="version mismatch")

            check_in = date.fromisoformat(body["check_in"])
            check_out = date.fromisoformat(body["check_out"])
            if (b.status, b.check_in, b.check_out) == (body["status"], check_in, check_out):
                return {"ok": True}

            b.status, b.check_in, b.check_out = body["status"], check_in, check_out
            b.version += 1
            self.stats["corrections_applied"] += 1
            # a host-side change fires a webhook on real channels too
            self._emit(b, "reservation.cancelled" if b.status == "cancelled" else "reservation.modified")
            return {"ok": True}

        @app.get("/listings/{listing}")
        async def get_listing(listing: str):
            if err := await self._maybe_fail():
                return err
            lst = self.listings[listing]
            return {
                "blocked": to_ranges(lst.blocked),
                "bookings": [
                    {
                        "reservation_id": b.reservation_id,
                        "status": b.status,
                        "version": b.version,
                        "check_in": b.check_in.isoformat(),
                        "check_out": b.check_out.isoformat(),
                        "guest": b.guest,
                    }
                    for b in lst.bookings.values()
                ],
            }

        return app
