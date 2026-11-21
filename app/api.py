from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app import metrics
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

    return app


app = create_app()
