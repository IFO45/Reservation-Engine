import asyncio
import logging
from app.database import state
from app.config import RECONCILIATION_INTERVAL_SECONDS

logger = logging.getLogger("reconciliation_worker")

async def reconciliation_loop():
    logger.info("Reconciliation worker started.")
    while True:
        try:
            await asyncio.sleep(RECONCILIATION_INTERVAL_SECONDS)
            if not state.db_pool or not state.redis_client:
                continue

            async with state.db_pool.acquire() as conn:
                async with conn.transaction():
                    # Identify expired holds and transition status atomically
                    expired_rows = await conn.fetch(
                        """
                        UPDATE reservations
                        SET status = 'EXPIRED'
                        WHERE status = 'PENDING' AND expires_at <= NOW()
                        RETURNING id, event_id, seats
                        """
                    )

            if expired_rows:
                # Restore available count in Redis
                pipeline = state.redis_client.pipeline()
                for row in expired_rows:
                    event_key = f"event:{row['event_id']}:available"
                    pipeline.incrby(event_key, row["seats"])
                    logger.info(
                        f"Restored {row['seats']} seats for event {row['event_id']} "
                        f"(reservation {row['id']})"
                    )
                await pipeline.execute()
                for row in expired_rows:
                    logger.info(
                        f"Restored {row['seats']} seats for event {row['event_id']} "
                        f"(reservation {row['id']})"
                    )

        except asyncio.CancelledError:
            logger.info("Reconciliation worker received cancellation request.")
            break
        except Exception as e:
            logger.error(f"Error in reconciliation worker: {str(e)}", exc_info=True)