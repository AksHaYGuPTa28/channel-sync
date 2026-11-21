from __future__ import annotations

import json
import os
from pathlib import Path

import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://sync:sync@localhost:54329/sync")
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "db" / "schema.sql"


async def _init_conn(con: asyncpg.Connection) -> None:
    await con.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def create_pool(min_size: int = 2, max_size: int = 40) -> asyncpg.Pool:
    return await asyncpg.create_pool(DATABASE_URL, min_size=min_size, max_size=max_size, init=_init_conn)


async def reset_schema() -> None:
    """Drop everything and reapply schema.sql. Tests and the sim only."""
    con = await asyncpg.connect(DATABASE_URL)
    try:
        await con.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await con.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
    finally:
        await con.close()
