import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://engine_user:engine_password@localhost:5432/reservation_db"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
HOLD_TTL_SECONDS = int(os.getenv("HOLD_TTL_SECONDS", "600"))  # 10 minutes
RECONCILIATION_INTERVAL_SECONDS = int(os.getenv("RECONCILIATION_INTERVAL_SECONDS", "30"))