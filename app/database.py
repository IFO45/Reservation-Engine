from contextlib import asynccontextmanager
import asyncpg
import redis.asyncio as aioredis
from app.config import DATABASE_URL, REDIS_URL

class AppState:
    db_pool: asyncpg.Pool = None
    redis_client: aioredis.Redis = None

state = AppState()

async def init_resources():
    state.db_pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=10,
        max_size=50,
        command_timeout=10.0
    )
    state.redis_client = aioredis.from_url(
        REDIS_URL,
        decode_responses=True,
        max_connections=50
    )

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