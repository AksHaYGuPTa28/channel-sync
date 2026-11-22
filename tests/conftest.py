import asyncio
import sys

import pytest_asyncio

from app.db import create_pool, reset_schema

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

CHANNELS = ("airbnb", "booking", "vrbo")


@pytest_asyncio.fixture
async def pool():
    await reset_schema()
    p = await create_pool(max_size=60)
    yield p
    await p.close()


@pytest_asyncio.fixture
async def unit(pool):
    tenant = await pool.fetchval("INSERT INTO tenants (name) VALUES ('t') RETURNING id")
    unit_id = await pool.fetchval(
        "INSERT INTO units (tenant_id, name) VALUES ($1, 'u1') RETURNING id", tenant
    )
    for ch in CHANNELS:
        await pool.execute(
            "INSERT INTO channel_listings (tenant_id, unit_id, channel, external_listing_id) VALUES ($1, $2, $3, $4)",
            tenant,
            unit_id,
            ch,
            f"{ch}-L1",
        )
    return unit_id
