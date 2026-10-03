from contextlib import asynccontextmanager
import asyncpg
import redis.asyncio as aioredis
from app.config import DATABASE_URL, REDIS_URL

class AppState:
    db_pool: asyncpg.Pool = None
    redis_client: aioredis.Redis = None

state = AppState()

async def seed_inventory():
    """
    Create each event's Redis counter from PostgreSQL if it does not exist yet.
    Counter = confirmed-adjusted seats minus ALL pending holds.
    (Expired-but-unswept holds are counted as held; the worker restores them
    when it sweeps them, so they are not restored twice.)
    SET ... NX means a live counter is never overwritten.
    """
    async with state.db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT e.id,
                   e.available_seats
                     - COALESCE(SUM(r.seats) FILTER (WHERE r.status = 'PENDING'), 0) AS free
            FROM events e
            LEFT JOIN reservations r ON r.event_id = e.id
            GROUP BY e.id, e.available_seats
            """
        )
    for row in rows:
        await state.redis_client.set(
            f"event:{row['id']}:available", int(row["free"]), nx=True
        )
        
async def init_resources():
    state.db_pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=10,
        max_size=50,
        command_timeout=10.0
    )
    
    # Use a blocking pool to queue requests instead of dropping them
    redis_pool = aioredis.BlockingConnectionPool.from_url(
        REDIS_URL,
        max_connections=100,
        timeout=10, # Seconds a request will wait for a free connection
        decode_responses=True
    )
    state.redis_client = aioredis.Redis(connection_pool=redis_pool)
    await seed_inventory()

async def close_resources():
    if state.redis_client:
        await state.redis_client.aclose()
    if state.db_pool:
        await state.db_pool.close()

async def get_db():
    async with state.db_pool.acquire() as connection:
        yield connection

async def get_redis():
    return state.redis_client