import uuid
import json
import hashlib
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, Header, HTTPException, status
import asyncpg
import redis.asyncio as aioredis

from app.config import HOLD_TTL_SECONDS
from app.database import init_resources, close_resources, get_db, get_redis, state
from app.schemas import HoldRequest, HoldResponse, ConfirmRequest, ConfirmResponse
from app.lua_scripts import RESERVE_SEATS_LUA
from app.worker import reconciliation_loop
import asyncio


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


# app/main.py

@app.post(
    "/api/reservations/hold",
    response_model=HoldResponse,
    status_code=status.HTTP_201_CREATED
)
async def hold_seats(
        req: HoldRequest,
        redis: aioredis.Redis = Depends(get_redis)  # Removed Depends(get_db)
):
    reservation_id = uuid.uuid4()
    event_key = f"event:{req.event_id}:available"
    reservation_key = f"reservation:{reservation_id}"

    expires_at = datetime.now(timezone.utc) + timedelta(seconds=HOLD_TTL_SECONDS)
    payload = json.dumps({
        "event_id": str(req.event_id),
        "user_id": str(req.user_id),
        "seats": req.seats
    })

    # 1. Atomic decrement in Redis FIRST (sheds 80%+ of load instantly)
    result = await redis.eval(
        RESERVE_SEATS_LUA,
        2,
        event_key,
        reservation_key,
        req.seats,
        HOLD_TTL_SECONDS,
        payload
    )

    if result == -1:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Event inventory not initialized in cache."
        )
    if result == 0:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Not enough seats available."
        )

    # 2. ONLY acquire a PostgreSQL connection if Redis reserved the seat
    async with state.db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO reservations (id, event_id, user_id, seats, status, expires_at)
            VALUES ($1, $2, $3, $4, 'PENDING', $5)
            """,
            reservation_id, req.event_id, req.user_id, req.seats, expires_at
        )

    return HoldResponse(
        reservation_id=reservation_id,
        expires_at=expires_at,
        message="Seats successfully reserved for 10 minutes."
    )


@app.post(
    "/api/reservations/confirm",
    response_model=ConfirmResponse,
    status_code=status.HTTP_200_OK
)
async def confirm_booking(
        req: ConfirmRequest,
        idempotency_key: str = Header(..., alias="Idempotency-Key"),
        conn: asyncpg.Connection = Depends(get_db),
        redis: aioredis.Redis = Depends(get_redis)
):
    req_hash = hashlib.sha256(
        json.dumps(req.model_dump(), default=str, sort_keys=True).encode()
    ).hexdigest()

    # 1. Check Idempotency Record
    cached = await conn.fetchrow(
        """
        SELECT response_body, status_code, request_hash 
        FROM idempotency_keys 
        WHERE key = $1 AND user_id = $2
        """,
        idempotency_key, req.user_id
    )

    if cached:
        if cached["request_hash"] != req_hash:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Idempotency key reused with mismatched request payload."
            )
        return json.loads(cached["response_body"])

    # 2. Begin ACID Transaction
    async with conn.transaction():
        # Lock target reservation row to avoid concurrent confirmations
        reservation = await conn.fetchrow(
            """
            SELECT * FROM reservations
            WHERE id = $1 AND user_id = $2 AND status = 'PENDING' AND expires_at > NOW()
            FOR UPDATE
            """,
            req.reservation_id, req.user_id
        )

        if not reservation:
            raise HTTPException(
                status_code=status.HTTP_410_GONE,
                detail="Reservation has expired or is invalid."
            )

        # Confirm hold and adjust base inventory
        await conn.execute(
            "UPDATE reservations SET status = 'CONFIRMED' WHERE id = $1",
            req.reservation_id
        )
        await conn.execute(
            "UPDATE events SET available_seats = available_seats - $1 WHERE id = $2",
            reservation["seats"], reservation["event_id"]
        )

        # Create finalized order
        order_id = uuid.uuid4()
        await conn.execute(
            """
            INSERT INTO orders (id, reservation_id, user_id, amount_cents, status)
            VALUES ($1, $2, $3, $4, 'COMPLETED')
            """,
            order_id, req.reservation_id, req.user_id, req.amount_cents
        )

        response_payload = {
            "order_id": str(order_id),
            "reservation_id": str(req.reservation_id),
            "seats": reservation["seats"],
            "status": "CONFIRMED"
        }

        # Store idempotency receipt valid for 24 hours
        key_expiry = datetime.now(timezone.utc) + timedelta(days=1)
        await conn.execute(
            """
            INSERT INTO idempotency_keys (key, user_id, request_hash, response_body, status_code, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            idempotency_key, req.user_id, req_hash, json.dumps(response_payload), 200, key_expiry
        )

    # 3. Clean up the Redis hold key outside the DB transaction
    await redis.delete(f"reservation:{req.reservation_id}")

    return response_payload