"""Control room backend. Starts sim runs, toggles outages, serves the dashboard.

python -m sim.server        # http://localhost:8000
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app import guest_comms
from sim.scenario import CHANNELS, ChaosRun, Params

DIST = Path(__file__).resolve().parent.parent / "dashboard" / "dist"

app = FastAPI(title="channel-sync control room")
current: dict = {"run": None, "task": None}
# an LLM drafts guest messages when ANTHROPIC_API_KEY is set (or GUEST_DRAFTER=llm)
drafter = guest_comms.make_drafter()


class RunRequest(BaseModel):
    actions: int = Field(3000, ge=100, le=20000)
    duration: float = Field(30, ge=5, le=120)
    seed: int = 7
    scheduled_outage: bool = True


class OutageRequest(BaseModel):
    down: bool


def _running() -> bool:
    return current["task"] is not None and not current["task"].done()


@app.post("/api/runs", status_code=202)
async def start_run(req: RunRequest):
    if _running():
        raise HTTPException(409, "A run is already in progress.")
    if current["run"]:
        await current["run"].close()

    run = ChaosRun(Params(**req.model_dump()), drafter=drafter)
    current["run"] = run
    current["task"] = asyncio.create_task(run.run())
    return {"phase": run.phase}


@app.post("/api/channels/{channel}/outage")
async def set_outage(channel: str, req: OutageRequest):
    if channel not in CHANNELS:
        raise HTTPException(404, f"Unknown channel {channel}.")
    run = current["run"]
    if not _running() or run.phase != "storm":
        raise HTTPException(409, "Outages can only be toggled while guests are booking.")
    run.set_outage(channel, req.down)
    return {"channel": channel, "down": req.down}


class ApproveRequest(BaseModel):
    body: str | None = None


def _pool():
    run = current["run"]
    if run is None or run.pool is None:
        raise HTTPException(409, "Start a run first.")
    return run.pool


@app.post("/api/inbox/{message_id}/approve")
async def approve_message(message_id: int, req: ApproveRequest):
    try:
        await guest_comms.approve(_pool(), message_id, req.body)
    except guest_comms.InboxError as exc:
        raise HTTPException(exc.status, str(exc)) from exc
    return {"ok": True}


@app.post("/api/inbox/{message_id}/discard")
async def discard_message(message_id: int):
    try:
        await guest_comms.discard(_pool(), message_id)
    except guest_comms.InboxError as exc:
        raise HTTPException(exc.status, str(exc)) from exc
    return {"ok": True}


@app.get("/api/state")
async def get_state():
    run, task = current["run"], current["task"]
    if run is None:
        return {"phase": "idle"}
    snap = await run.snapshot()
    if task.done() and task.exception():
        snap["phase"] = "failed"
        snap["error"] = repr(task.exception())
    return snap


if DIST.exists():
    app.mount("/assets", StaticFiles(directory=DIST / "assets"), name="assets")

    @app.get("/{path:path}")
    async def index(path: str):
        # assets have hashed names, but index.html doesn't, so make browsers revalidate it
        return FileResponse(DIST / "index.html", headers={"Cache-Control": "no-cache"})


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    print(f"http://localhost:{port}  (guest messages drafted by {type(drafter).__name__})")
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    asyncio.run(uvicorn.Server(config).serve())
