from __future__ import annotations

from datetime import date

import httpx

from app.resilience import CircuitBreaker, TokenBucket


class ChannelError(Exception):
    def __init__(self, message: str, retryable: bool = True):
        super().__init__(message)
        self.retryable = retryable


class VersionConflict(Exception):
    """Channel has a newer version of the reservation than the one we're correcting."""


class ChannelClient:
    def __init__(self, clients: dict[str, httpx.AsyncClient], rate_per_sec: float = 200, burst: int = 50):
        self.clients = clients
        self.buckets = {ch: TokenBucket(rate_per_sec, burst) for ch in clients}
        self.breakers = {ch: CircuitBreaker() for ch in clients}

    async def _request(self, channel: str, method: str, path: str, json=None) -> httpx.Response:
        await self.buckets[channel].acquire()
        try:
            resp = await self.clients[channel].request(method, path, json=json, timeout=5.0)
        except httpx.HTTPError as exc:
            raise ChannelError(f"{channel}: {type(exc).__name__}") from exc

        if resp.status_code == 409:
            raise VersionConflict(resp.text)
        if resp.status_code == 429 or resp.status_code >= 500:
            raise ChannelError(f"{channel}: HTTP {resp.status_code}")
        if resp.status_code >= 400:
            raise ChannelError(f"{channel}: HTTP {resp.status_code} {resp.text}", retryable=False)
        return resp

    async def put_calendar(self, channel: str, listing: str, blocked: list[tuple[date, date]]) -> None:
        body = {"blocked": [[s.isoformat(), e.isoformat()] for s, e in blocked]}
        await self._request(channel, "PUT", f"/listings/{listing}/calendar", json=body)

    async def correct_reservation(self, channel: str, listing: str, payload: dict) -> None:
        rid = payload["reservation_id"]
        await self._request(channel, "POST", f"/listings/{listing}/reservations/{rid}/correct", json=payload)

    async def send_guest_message(self, channel: str, listing: str, payload: dict) -> None:
        rid = payload["reservation_id"]
        body = {"message_id": payload["message_id"], "body": payload["body"]}
        await self._request(channel, "POST", f"/listings/{listing}/reservations/{rid}/messages", json=body)

    async def get_listing_state(self, channel: str, listing: str) -> dict:
        resp = await self._request(channel, "GET", f"/listings/{listing}")
        return resp.json()
