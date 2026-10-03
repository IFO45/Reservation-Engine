"""
Benchmark-only baselines. NOT part of the real API.

Enabled only when ENABLE_BENCH_ROUTES=1 (see docker-compose.bench.yml).
They use separate tables (bench_events, bench_holds) so they never touch real data.
"""
import uuid

from fastapi import APIRouter, HTTPException

from app.database import state
from app.schemas import HoldRequest

router = APIRouter(prefix="/bench", tags=["benchmark"])


@router.post("/naive/hold", status_code=201)
async def naive_hold(req: HoldRequest):
    """
    Classic check-then-act bug: read the count, decide in Python, write it back.
    Many requests read the same stale value, so this oversells under concurrency.
    """
    async with state.db_pool.acquire() as conn:
        remaining = await conn.fetchval(
            "SELECT remaining FROM bench_events WHERE id = $1", req.event_id
        )
        if remaining is None or remaining < req.seats:
            raise HTTPException(status_code=409, detail="Not enough seats available.")

        await conn.execute(
            "UPDATE bench_events SET remaining = $1 WHERE id = $2",
            remaining - req.seats, req.event_id,
        )
        await conn.execute(
            "INSERT INTO bench_holds (id, event_id, seats) VALUES ($1, $2, $3)",
            uuid.uuid4(), req.event_id, req.seats,
        )
    return {"ok": True}


@router.post("/pg/hold", status_code=201)
async def pg_atomic_hold(req: HoldRequest):
    """
    Correct single-statement approach: conditional UPDATE in one transaction.
    No Redis; every request (including rejections) goes through the Postgres row lock.
    """
    async with state.db_pool.acquire() as conn:
        async with conn.transaction():
            updated = await conn.fetchval(
                """
                UPDATE bench_events
                SET remaining = remaining - $1
                WHERE id = $2 AND remaining >= $1
                RETURNING remaining
                """,
                req.seats, req.event_id,
            )
            if updated is None:
                raise HTTPException(status_code=409, detail="Not enough seats available.")
            await conn.execute(
                "INSERT INTO bench_holds (id, event_id, seats) VALUES ($1, $2, $3)",
                uuid.uuid4(), req.event_id, req.seats,
            )
    return {"ok": True}
