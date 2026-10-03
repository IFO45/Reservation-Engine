"""
Tests for the hold-expiry worker.

These require the API to run with a short worker interval:
    docker compose -f docker-compose.yml -f docker-compose.test.yml up --build -d
"""
import asyncio
import random
import time
import uuid
from collections import Counter

import asyncpg
import httpx
import redis.asyncio as aioredis

BASE_URL = "http://localhost:8000"
DB_URL = "postgresql://engine_user:engine_password@localhost:5432/reservation_db"
REDIS_URL = "redis://localhost:6379/0"
EVENT_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
REDIS_KEY = f"event:{EVENT_ID}:available"

TOTAL_SEATS = 20

# Must match RECONCILIATION_INTERVAL_SECONDS in docker-compose.test.yml
WORKER_INTERVAL = 2

HINT = "Is the API running with RECONCILIATION_INTERVAL_SECONDS=2? See docker-compose.test.yml."


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
async def reset_state(total_seats: int = TOTAL_SEATS):
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    conn = await asyncpg.connect(DB_URL)
    try:
        await r.flushdb()
        await conn.execute("TRUNCATE reservations, orders, idempotency_keys CASCADE;")
        await conn.execute(
            "UPDATE events SET total_seats = $1, available_seats = $1 WHERE id = $2",
            total_seats, EVENT_ID,
        )
        await r.set(REDIS_KEY, total_seats)
    finally:
        await conn.close()
        await r.aclose()


async def db_fetchval(query: str, *args):
    conn = await asyncpg.connect(DB_URL)
    try:
        return await conn.fetchval(query, *args)
    finally:
        await conn.close()


async def db_execute(query: str, *args):
    conn = await asyncpg.connect(DB_URL)
    try:
        return await conn.execute(query, *args)
    finally:
        await conn.close()


async def redis_get_int(key: str) -> int:
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        return int(await r.get(key))
    finally:
        await r.aclose()


async def wait_for(check, timeout: float) -> bool:
    """Poll an async condition until it is true or the timeout passes."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await check():
            return True
        await asyncio.sleep(0.25)
    return False


async def create_hold(client: httpx.AsyncClient, seats: int = 1, user_id: str | None = None) -> str:
    res = await client.post("/api/reservations/hold", json={
        "event_id": EVENT_ID,
        "user_id": user_id or str(uuid.uuid4()),
        "seats": seats,
    })
    assert res.status_code == 201, f"Setup failed: hold returned {res.status_code} {res.text}"
    return res.json()["reservation_id"]


import pytest


@pytest.fixture(autouse=True)
async def clean_state():
    await reset_state()
    yield


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------
async def test_worker_restores_expired_seats_exactly_once():
    """An expired hold returns its seats to Redis once, and never a second time."""
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        reservation_id = await create_hold(client, seats=3)

    assert await redis_get_int(REDIS_KEY) == TOTAL_SEATS - 3

    await db_execute(
        "UPDATE reservations SET expires_at = NOW() - INTERVAL '1 minute' WHERE id = $1",
        reservation_id,
    )

    async def restored():
        return await redis_get_int(REDIS_KEY) == TOTAL_SEATS

    assert await wait_for(restored, timeout=WORKER_INTERVAL * 3 + 5), f"Seats were not restored. {HINT}"

    status = await db_fetchval("SELECT status FROM reservations WHERE id = $1", reservation_id)
    assert status == "EXPIRED"

    # Let several more worker passes run: the counter must not grow again.
    await asyncio.sleep(WORKER_INTERVAL * 3)
    assert await redis_get_int(REDIS_KEY) == TOTAL_SEATS, "Seats were restored more than once"


async def test_worker_ignores_confirmed_reservations():
    """A paid reservation must stay CONFIRMED and keep its seats, even if expires_at is in the past."""
    user_id = str(uuid.uuid4())

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        reservation_id = await create_hold(client, seats=3, user_id=user_id)
        res = await client.post(
            "/api/reservations/confirm",
            headers={"Idempotency-Key": str(uuid.uuid4())},
            json={"reservation_id": reservation_id, "user_id": user_id, "amount_cents": 5000},
        )
        assert res.status_code == 200, res.text

    await db_execute(
        "UPDATE reservations SET expires_at = NOW() - INTERVAL '1 minute' WHERE id = $1",
        reservation_id,
    )

    await asyncio.sleep(WORKER_INTERVAL * 3)

    status = await db_fetchval("SELECT status FROM reservations WHERE id = $1", reservation_id)
    assert status == "CONFIRMED", f"Worker changed a paid reservation to {status}"
    assert await redis_get_int(REDIS_KEY) == TOTAL_SEATS - 3, "Worker restored seats for a paid reservation"


async def test_confirm_racing_worker_never_double_counts():
    """
    20 holds expire at staggered moments while confirms arrive around each expiry,
    with the worker sweeping every 2 seconds. Every seat must end up in exactly one
    place: sold (CONFIRMED + order) or returned (EXPIRED + restored in Redis).

    This does not force one specific interleaving, so run it repeatedly.
    """
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        holds = []
        for _ in range(TOTAL_SEATS):
            user_id = str(uuid.uuid4())
            holds.append((await create_hold(client, seats=1, user_id=user_id), user_id))

        # Stagger expiries over the next ~1-3 seconds.
        conn = await asyncpg.connect(DB_URL)
        try:
            for i, (reservation_id, _) in enumerate(holds):
                await conn.execute(
                    "UPDATE reservations "
                    "SET expires_at = NOW() + ($1::float8 * INTERVAL '1 second') "
                    "WHERE id = $2",
                    1.0 + i * 0.1, reservation_id,
                )
        finally:
            await conn.close()

        async def confirm(i, reservation_id, user_id):
            # Fire the confirm close to this hold's expiry, with jitter either side.
            await asyncio.sleep(max(0.0, 1.0 + i * 0.1 + random.uniform(-0.3, 0.3)))
            res = await client.post(
                "/api/reservations/confirm",
                headers={"Idempotency-Key": str(uuid.uuid4())},
                json={"reservation_id": reservation_id, "user_id": user_id, "amount_cents": 5000},
            )
            return reservation_id, res.status_code

        results = await asyncio.gather(
            *(confirm(i, rid, uid) for i, (rid, uid) in enumerate(holds))
        )

    codes = Counter(code for _, code in results)
    assert set(codes) <= {200, 410}, f"Unexpected status codes: {dict(codes)}"
    confirmed = codes[200]

    # Let the worker sweep anything still pending.
    await asyncio.sleep(WORKER_INTERVAL * 2 + 1)

    pending = await db_fetchval("SELECT COUNT(*) FROM reservations WHERE status = 'PENDING'")
    assert pending == 0, f"{pending} holds were never resolved. {HINT}"

    for reservation_id, code in results:
        status = await db_fetchval("SELECT status FROM reservations WHERE id = $1", reservation_id)
        expected = "CONFIRMED" if code == 200 else "EXPIRED"
        assert status == expected, f"Reservation {reservation_id}: HTTP {code} but status {status}"

    orders = await db_fetchval("SELECT COUNT(*) FROM orders")
    assert orders == confirmed, f"{confirmed} confirmations but {orders} orders"

    assert await redis_get_int(REDIS_KEY) == TOTAL_SEATS - confirmed, "Redis counter does not match sold seats"
    remaining = await db_fetchval("SELECT available_seats FROM events WHERE id = $1", EVENT_ID)
    assert remaining == TOTAL_SEATS - confirmed, "Database inventory does not match sold seats"
