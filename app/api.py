from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from app import guest_comms, metrics
from app.db import create_pool
from app.ingest import ChannelEvent, apply_event


def create_app(pool=None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        owns_pool = app.state.pool is None
        if owns_pool:
            app.state.pool = await create_pool()
        yield
        if owns_pool:
            await app.state.pool.close()

    app = FastAPI(title="channel-sync", lifespan=lifespan)
    app.state.pool = pool

    @app.post("/webhooks/{channel}")
    async def webhook(channel: str, body: dict, request: Request):
        # 200 for duplicates/stale too, otherwise the channel keeps retrying them
        ev = ChannelEvent(channel=channel, **body)
        return {"outcome": await apply_event(request.app.state.pool, ev)}

    @app.get("/health")
    async def health():
        return {"ok": True}

    @app.get("/metrics")
    async def get_metrics(request: Request):
        return await metrics.snapshot(request.app.state.pool)

    # operator inbox: drafted messages to moved guests, sent only once approved

    @app.get("/inbox")
    async def get_inbox(request: Request):
        return await guest_comms.inbox(request.app.state.pool)

    @app.post("/inbox/{message_id}/approve")
    async def approve(message_id: int, req: ApproveRequest, request: Request):
        try:
            await guest_comms.approve(request.app.state.pool, message_id, req.body)
        except guest_comms.InboxError as exc:
            raise HTTPException(exc.status, str(exc)) from exc
        return {"ok": True}

    @app.post("/inbox/{message_id}/discard")
    async def discard(message_id: int, request: Request):
        try:
            await guest_comms.discard(request.app.state.pool, message_id)
        except guest_comms.InboxError as exc:
            raise HTTPException(exc.status, str(exc)) from exc
        return {"ok": True}

    return app


class ApproveRequest(BaseModel):
    body: str | None = None  # operator's edited text; omit to send the draft as is


app = create_app()
