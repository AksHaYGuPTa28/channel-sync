from __future__ import annotations

import asyncpg

DOUBLE_BOOKINGS_SQL = """
SELECT count(*)
FROM reservations a
JOIN reservations b ON a.unit_id = b.unit_id AND a.id < b.id AND a.stay && b.stay
WHERE a.status = 'confirmed' AND b.status = 'confirmed'
"""

LATENCY_SQL = """
SELECT
    percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - created_at)) * 1000 AS p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - created_at)) * 1000 AS p95,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - created_at)) * 1000 AS p99,
    sum(attempts - 1) AS retries
FROM outbox
WHERE status = 'done'
"""


async def _counts(pool: asyncpg.Pool, table: str, column: str) -> dict[str, int]:
    rows = await pool.fetch(f"SELECT {column} AS k, count(*) AS n FROM {table} GROUP BY {column}")
    return {(r["k"] or "processing"): r["n"] for r in rows}


async def snapshot(pool: asyncpg.Pool) -> dict:
    latency = await pool.fetchrow(LATENCY_SQL)
    return {
        "double_bookings": await pool.fetchval(DOUBLE_BOOKINGS_SQL),
        "events": await _counts(pool, "inbound_events", "outcome"),
        "reservations": await _counts(pool, "reservations", "status"),
        "conflicts": await _counts(pool, "conflicts", "kind"),
        "outbox": await _counts(pool, "outbox", "status"),
        "push_latency_ms": {k: round(latency[k] or 0, 1) for k in ("p50", "p95", "p99")},
        "push_retries": int(latency["retries"] or 0),
    }
