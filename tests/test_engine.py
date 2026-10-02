import asyncio
import uuid
from collections import Counter

import asyncpg
import httpx
import pytest
import redis.asyncio as aioredis

BASE_URL = "http://localhost:8000"
DB_URL = "postgresql://engine_user:engine_password@localhost:5432/reservation_db"
REDIS_URL = "redis://localhost:6379/0"
EVENT_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
REDIS_KEY = f"event:{EVENT_ID}:available"

TOTAL_SEATS = 20

# Max requests in flight at once from the test client.
# httpx defaults to 100, which quietly throttles a "1,000 request" test.
LIMITS = httpx.Limits(max_connections=200, max_keepalive_connections=200)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
async def reset_state(total_seats: int = TOTAL_SEATS):
    """Wipe Postgres + Redis and seed exactly `total_seats` for the test event."""
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


async def fire_together(make_request, n: int):
    """
    Create n tasks that all wait on a gate, then release them at once.
    `make_request` is an async function taking the task index.
    Returns the list of results in task order.
    """
    gate = asyncio.Event()

    async def runner(i):
        await gate.wait()
        return await make_request(i)

    tasks = [asyncio.create_task(runner(i)) for i in range(n)]
    await asyncio.sleep(0)  # let every task reach the gate
    gate.set()
    return await asyncio.gather(*tasks)


async def db_fetchval(query: str, *args):
    conn = await asyncpg.connect(DB_URL)
    try:
        return await conn.fetchval(query, *args)
    finally:
        await conn.close()


async def redis_get_int(key: str) -> int:
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        return int(await r.get(key))
    finally:
        await r.aclose()


@pytest.fixture(autouse=True)
async def clean_state():
    """Every test starts with exactly TOTAL_SEATS seats and no leftover rows."""
    await reset_state()
    yield


# --------------------------------------------------------------------------
# Hold tests
# --------------------------------------------------------------------------
async def test_concurrent_single_seat_holds():
    """1,000 buyers, 20 seats, 1 seat each: exactly 20 holds, no oversell, no errors."""
    total_requests = 1000

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=60.0, limits=LIMITS) as client:

        async def attack(_):
            res = await client.post("/api/reservations/hold", json={
                "event_id": EVENT_ID,
                "user_id": str(uuid.uuid4()),
                "seats": 1,
            })
            return res.status_code

        codes = await fire_together(attack, total_requests)

    tally = Counter(codes)

    # Check for unexpected codes FIRST so the failure message shows what came back.
    unexpected = {c: n for c, n in tally.items() if c not in (201, 409)}
    assert not unexpected, f"Unexpected status codes: {unexpected} (full tally: {dict(tally)})"

    assert tally[201] == TOTAL_SEATS, f"Expected {TOTAL_SEATS} holds, tally: {dict(tally)}"
    assert tally[409] == total_requests - TOTAL_SEATS

    # Durable ledger must agree with Redis.
    db_seats = await db_fetchval(
        "SELECT COALESCE(SUM(seats), 0) FROM reservations "
        "WHERE event_id = $1 AND status = 'PENDING'",
        EVENT_ID,
    )
    assert db_seats == TOTAL_SEATS, f"DB drift: {tally[201]} holds granted but DB has {db_seats} seats held"
    assert await redis_get_int(REDIS_KEY) == 0


async def test_concurrent_multi_seat_holds():
    """50 buyers want 3 seats each, 10 available: 3 succeed (9 seats), 1 seat left over."""
    await reset_state(total_seats=10)

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0, limits=LIMITS) as client:

        async def attack(_):
            res = await client.post("/api/reservations/hold", json={
                "event_id": EVENT_ID,
                "user_id": str(uuid.uuid4()),
                "seats": 3,
            })
            return res.status_code

        codes = await fire_together(attack, 50)

    tally = Counter(codes)
    unexpected = {c: n for c, n in tally.items() if c not in (201, 409)}
    assert not unexpected, f"Unexpected status codes: {unexpected} (full tally: {dict(tally)})"

    assert tally[201] == 3
    assert tally[409] == 47

    # Counter must never go negative, and must match the DB.
    assert await redis_get_int(REDIS_KEY) == 1
    db_seats = await db_fetchval(
        "SELECT COALESCE(SUM(seats), 0) FROM reservations "
        "WHERE event_id = $1 AND status = 'PENDING'",
        EVENT_ID,
    )
    assert db_seats == 9


# --------------------------------------------------------------------------
# Confirm / idempotency tests
# --------------------------------------------------------------------------
async def _create_hold(client: httpx.AsyncClient, user_id: str, seats: int = 2) -> str:
    res = await client.post("/api/reservations/hold", json={
        "event_id": EVENT_ID, "user_id": user_id, "seats": seats,
    })
    assert res.status_code == 201, f"Setup failed: hold returned {res.status_code} {res.text}"
    return res.json()["reservation_id"]


async def test_idempotent_concurrent_payments():
    """
    20 simultaneous confirms with the same Idempotency-Key.
    Requirements:
      - no 5xx responses
      - at least one 200, the rest are 200 (cached) or 409 (in progress)
      - exactly one order, seats confirmed once
      - a retry after the burst returns the same receipt
    If your API waits for the first request and returns 200 to all duplicates,
    tighten ALLOWED to {200}.
    """
    ALLOWED = {200, 409}
    user_id = str(uuid.uuid4())
    idempotency_key = str(uuid.uuid4())
    payload = {"user_id": user_id, "amount_cents": 5000}

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0, limits=LIMITS) as client:
        reservation_id = await _create_hold(client, user_id, seats=2)
        payload["reservation_id"] = reservation_id

        async def pay(_):
            return await client.post(
                "/api/reservations/confirm",
                headers={"Idempotency-Key": idempotency_key},
                json=payload,
            )

        responses = await fire_together(pay, 20)

        tally = Counter(r.status_code for r in responses)
        unexpected = {c: n for c, n in tally.items() if c not in ALLOWED}
        assert not unexpected, f"Unexpected status codes: {unexpected} (full tally: {dict(tally)})"
        assert tally[200] >= 1, f"No request succeeded: {dict(tally)}"

        # All successful responses must carry the same receipt.
        bodies = [r.json() for r in responses if r.status_code == 200]
        assert all(b == bodies[0] for b in bodies), f"200 responses differ: {bodies}"

        # Retry after the burst: must replay the same receipt
        retry = await pay(0)
        assert retry.status_code == 200, f"Retry returned {retry.status_code}: {retry.text}"
        assert retry.json() == bodies[0]

    # Durable state: one order, one seat decrement.
    order_count = await db_fetchval(
        "SELECT COUNT(*) FROM orders WHERE reservation_id = $1", reservation_id
    )
    assert order_count == 1, f"IDEMPOTENCY FAILURE: {order_count} orders created"

    # Adjust the column name if you renamed it (e.g. seats_confirmed).
    remaining = await db_fetchval("SELECT available_seats FROM events WHERE id = $1", EVENT_ID)
    assert remaining == TOTAL_SEATS - 2, f"Seats decremented incorrectly: {remaining} left"


async def test_idempotency_key_reuse_with_different_payload():
    """Same key, different payload must be rejected with 422 and must not create a second order."""
    user_id = str(uuid.uuid4())
    key = str(uuid.uuid4())

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        reservation_id = await _create_hold(client, user_id, seats=1)
        headers = {"Idempotency-Key": key}

        first = await client.post("/api/reservations/confirm", headers=headers, json={
            "reservation_id": reservation_id, "user_id": user_id, "amount_cents": 5000,
        })
        assert first.status_code == 200, first.text

        second = await client.post("/api/reservations/confirm", headers=headers, json={
            "reservation_id": reservation_id, "user_id": user_id, "amount_cents": 9999,
        })
        assert second.status_code == 422, f"Got {second.status_code}: {second.text}"

    order_count = await db_fetchval(
        "SELECT COUNT(*) FROM orders WHERE reservation_id = $1", reservation_id
    )
    assert order_count == 1


async def test_confirm_rejected_after_hold_expires():
    """An expired hold must not be confirmable, even if the worker hasn't run yet."""
    user_id = str(uuid.uuid4())

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        reservation_id = await _create_hold(client, user_id, seats=1)

        # Force the hold into the past without touching its status.
        conn = await asyncpg.connect(DB_URL)
        try:
            await conn.execute(
                "UPDATE reservations SET expires_at = NOW() - INTERVAL '1 minute' WHERE id = $1",
                reservation_id,
            )
        finally:
            await conn.close()

        res = await client.post(
            "/api/reservations/confirm",
            headers={"Idempotency-Key": str(uuid.uuid4())},
            json={"reservation_id": reservation_id, "user_id": user_id, "amount_cents": 5000},
        )

    assert res.status_code in (409, 410), f"Expired hold was accepted: {res.status_code} {res.text}"
    order_count = await db_fetchval(
        "SELECT COUNT(*) FROM orders WHERE reservation_id = $1", reservation_id
    )
    assert order_count == 0