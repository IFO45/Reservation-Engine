import asyncio
import hashlib
import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import asyncpg
import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.responses import JSONResponse

from app.config import HOLD_TTL_SECONDS
from app.database import init_resources, close_resources, get_db, get_redis, state
from app.schemas import HoldRequest, HoldResponse, ConfirmRequest, ConfirmResponse
from app.lua_scripts import RESERVE_SEATS_LUA
from app.worker import reconciliation_loop

logger = logging.getLogger("reservation_engine")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: allocate connection pools & kick off background task
    await init_resources()
    worker_task = asyncio.create_task(reconciliation_loop())
    yield
    # Shutdown: cancel background task & cleanly drain pools
    worker_task.cancel()
    await asyncio.gather(worker_task, return_exceptions=True)
    await close_resources()


app = FastAPI(title="High-Concurrency Reservation Engine", lifespan=lifespan)
import os
if os.getenv("ENABLE_BENCH_ROUTES") == "1":
    from app.bench_routes import router as bench_router
    app.include_router(bench_router)

# ---------------------------------------------------------------------------
# Phase 1: temporary hold
# ---------------------------------------------------------------------------
@app.post(
    "/api/reservations/hold",
    response_model=HoldResponse,
    status_code=status.HTTP_201_CREATED,
)
async def hold_seats(
    req: HoldRequest,
    redis: aioredis.Redis = Depends(get_redis),
):
    reservation_id = uuid.uuid4()
    event_key = f"event:{req.event_id}:available"
    reservation_key = f"reservation:{reservation_id}"

    expires_at = datetime.now(timezone.utc) + timedelta(seconds=HOLD_TTL_SECONDS)
    payload = json.dumps({
        "event_id": str(req.event_id),
        "user_id": str(req.user_id),
        "seats": req.seats,
    })

    # 1. Atomic check-and-decrement in Redis. Rejected requests never touch Postgres.
    result = await redis.eval(
        RESERVE_SEATS_LUA, 2, event_key, reservation_key, req.seats, HOLD_TTL_SECONDS, payload
    )

    if result == -1:
        raise HTTPException(status_code=404, detail="Event inventory not initialized.")
    if result == 0:
        raise HTTPException(status_code=409, detail="Not enough seats available.")

    # 2. Persist the hold. If this fails, compensate so the seats are not leaked.
    try:
        async with state.db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO reservations (id, event_id, user_id, seats, status, expires_at)
                VALUES ($1, $2, $3, $4, 'PENDING', $5)
                """,
                reservation_id, req.event_id, req.user_id, req.seats, expires_at,
            )
    except Exception:
        logger.exception(
            "Hold insert failed; releasing %s seats for event %s", req.seats, req.event_id
        )
        await redis.incrby(event_key, req.seats)
        await redis.delete(reservation_key)
        raise HTTPException(
            status_code=500,
            detail="Database transaction failed. Inventory hold released.",
        )

    return HoldResponse(
        reservation_id=reservation_id,
        expires_at=expires_at,
        message="Seats successfully reserved for 10 minutes.",
    )


# ---------------------------------------------------------------------------
# Phase 2: payment + confirmation (insert-first idempotency)
#
# Schema requirements for idempotency_keys:
#   - UNIQUE / PRIMARY KEY on (key, user_id)
#   - response_body and status_code must be NULLABLE
#     (status_code IS NULL means "request still in progress")
# ---------------------------------------------------------------------------
@app.post(
    "/api/reservations/confirm",
    response_model=ConfirmResponse,
    status_code=status.HTTP_200_OK,
)
async def confirm_booking(
    req: ConfirmRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    conn: asyncpg.Connection = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    req_hash = hashlib.sha256(
        json.dumps(req.model_dump(), default=str, sort_keys=True).encode()
    ).hexdigest()
    key_expiry = datetime.now(timezone.utc) + timedelta(days=1)

    # 1. Claim the key FIRST. Only one concurrent request can win this insert.
    claimed = await conn.fetchval(
        """
        INSERT INTO idempotency_keys (key, user_id, request_hash, expires_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
        RETURNING 1
        """,
        idempotency_key, req.user_id, req_hash, key_expiry,
    )

    if not claimed:
        existing = await conn.fetchrow(
            """
            SELECT request_hash, response_body, status_code
            FROM idempotency_keys
            WHERE key = $1 AND user_id = $2
            """,
            idempotency_key, req.user_id,
        )
        if existing is None:
            # Winner failed and released the key between our two queries.
            raise HTTPException(status_code=409, detail="Request in progress, retry shortly.")
        if existing["request_hash"] != req_hash:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Idempotency key reused with mismatched request payload.",
            )
        if existing["status_code"] is None:
            raise HTTPException(status_code=409, detail="Request in progress, retry shortly.")
        return ConfirmResponse(**json.loads(existing["response_body"]))

    # 2. We own the key: do the real work in one ACID transaction.
    try:
        async with conn.transaction():
            reservation = await conn.fetchrow(
                """
                SELECT * FROM reservations
                WHERE id = $1 AND user_id = $2 AND status = 'PENDING' AND expires_at > NOW()
                FOR UPDATE
                """,
                req.reservation_id, req.user_id,
            )

            if not reservation:
                raise HTTPException(
                    status_code=status.HTTP_410_GONE,
                    detail="Reservation has expired or is invalid.",
                )

            await conn.execute(
                "UPDATE reservations SET status = 'CONFIRMED' WHERE id = $1",
                req.reservation_id,
            )
            updated = await conn.execute(
                "UPDATE events SET available_seats = available_seats - $1 "
                "WHERE id = $2 AND available_seats >= $1",
                reservation["seats"], reservation["event_id"],
            )
            if updated != "UPDATE 1":
                raise HTTPException(statuSs_code=409, detail="Inventory exhausted.")

            order_id = uuid.uuid4()
            await conn.execute(
                """
                INSERT INTO orders (id, reservation_id, user_id, amount_cents, status)
                VALUES ($1, $2, $3, $4, 'COMPLETED')
                """,
                order_id, req.reservation_id, req.user_id, req.amount_cents,
            )

            response_payload = {
                "order_id": str(order_id),
                "reservation_id": str(req.reservation_id),
                "seats": reservation["seats"],
                "status": "CONFIRMED",
            }

            # Save the receipt in the same transaction as the order.
            await conn.execute(
                """
                UPDATE idempotency_keys
                SET response_body = $1, status_code = 200
                WHERE key = $2 AND user_id = $3
                """,
                json.dumps(response_payload), idempotency_key, req.user_id,
            )
    except Exception:
        # Release the claim so the client can retry (e.g. after a transient error).
        # Known limitation: a process crash here leaves a stuck IN_PROGRESS row;
        # a cleanup job can reclaim rows past expires_at.
        await conn.execute(
            """
            DELETE FROM idempotency_keys
            WHERE key = $1 AND user_id = $2 AND status_code IS NULL
            """,
            idempotency_key, req.user_id,
        )
        raise

    # 3. Clean up the Redis hold key outside the DB transaction.
    await redis.delete(f"reservation:{req.reservation_id}")

    return response_payload