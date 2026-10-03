"""
Runs every variant x scenario benchmark and prints a markdown table.

Prerequisites:
  - k6 installed and on PATH
  - stack started with:
      docker compose -f docker-compose.yml -f docker-compose.bench.yml up --build -d
  - pip install asyncpg redis
Usage:
  python run_bench.py            # all variants, all scenarios
  python run_bench.py engine     # one variant
"""
import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import asyncpg
import redis.asyncio as aioredis

BASE_URL = "http://localhost:8000"
DB_URL = "postgresql://engine_user:engine_password@localhost:5432/reservation_db"
REDIS_URL = "redis://localhost:6379/0"
EVENT_ID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
REDIS_KEY = f"event:{EVENT_ID}:available"

K6_SCRIPT = Path(__file__).with_name("bench.js")
SUMMARY_FILE = Path("k6_summary.json")

VARIANTS = {
    "naive": "Naive read-then-write",
    "pg": "Atomic Postgres UPDATE",
    "engine": "Redis Lua + Postgres (this project)",
}
SCENARIOS = {
    "burst": {"seats": 20, "label": "Burst: 1,000 requests / 20 seats"},
    "soldout": {"seats": 20, "label": "Sustained 15s / 20 seats"},
    "stock": {"seats": 1_000_000, "label": "Sustained 15s / 1M seats"},
}

DDL = """
CREATE TABLE IF NOT EXISTS bench_events (id uuid PRIMARY KEY, remaining int NOT NULL);
CREATE TABLE IF NOT EXISTS bench_holds (id uuid PRIMARY KEY, event_id uuid NOT NULL, seats int NOT NULL);
"""


async def reset(seats: int):
    conn = await asyncpg.connect(DB_URL)
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        await conn.execute(DDL)
        await conn.execute("TRUNCATE bench_holds;")
        await conn.execute("TRUNCATE reservations, orders, idempotency_keys CASCADE;")
        await conn.execute(
            "INSERT INTO bench_events (id, remaining) VALUES ($1, $2) "
            "ON CONFLICT (id) DO UPDATE SET remaining = EXCLUDED.remaining",
            EVENT_ID, seats,
        )
        await conn.execute(
            "UPDATE events SET total_seats = $1, available_seats = $1 WHERE id = $2",
            seats, EVENT_ID,
        )
        await r.flushdb()
        await r.set(REDIS_KEY, seats)
    finally:
        await conn.close()
        await r.aclose()


async def seats_sold(variant: str) -> int:
    if variant == "engine":
        query = ("SELECT COALESCE(SUM(seats), 0) FROM reservations "
                 "WHERE event_id = $1 AND status = 'PENDING'")
    else:
        query = "SELECT COALESCE(SUM(seats), 0) FROM bench_holds WHERE event_id = $1"
    conn = await asyncpg.connect(DB_URL)
    try:
        return int(await conn.fetchval(query, EVENT_ID))
    finally:
        await conn.close()


def vals(metric: dict) -> dict:
    """Works with both the legacy {'values': {...}} and flattened summary formats."""
    return metric.get("values", metric)


def run_k6(variant: str, scenario: str) -> dict:
    env = {
        **os.environ,
        "VARIANT": variant,
        "SCENARIO": scenario,
        "BASE_URL": BASE_URL,
        "EVENT_ID": EVENT_ID,
        "OUT": str(SUMMARY_FILE),
    }
    subprocess.run(["k6", "run", "--quiet", str(K6_SCRIPT)], env=env, check=True)
    data = json.loads(SUMMARY_FILE.read_text())
    m = data["metrics"]

    def count(name):
        return int(vals(m[name]).get("count", 0)) if name in m else 0

    return {
        "rps": vals(m["http_reqs"])["rate"],
        "p95": vals(m["http_req_duration"])["p(95)"],
        "granted": count("hold_201"),
        "rejected": count("hold_409"),
        "errors": count("hold_other"),
    }


async def main():
    chosen = sys.argv[1:] or list(VARIANTS)
    rows = []
    for variant in chosen:
        for scenario, cfg in SCENARIOS.items():
            print(f"\n>>> {VARIANTS[variant]} | {cfg['label']}", flush=True)
            await reset(cfg["seats"])
            result = run_k6(variant, scenario)
            sold = await seats_sold(variant)
            rows.append((variant, scenario, result, sold, max(0, sold - cfg["seats"])))
            time.sleep(3)  # let the server settle between runs

    lines = [
        "| Variant | Scenario | req/s | p95 (ms) | 201 | 409 | errors | seats sold | oversold |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for variant, scenario, res, sold, oversold in rows:
        lines.append(
            f"| {VARIANTS[variant]} | {SCENARIOS[scenario]['label']} | {res['rps']:.0f} | "
            f"{res['p95']:.0f} | {res['granted']} | {res['rejected']} | {res['errors']} | "
            f"{sold} | {oversold} |"
        )
    table = "\n".join(lines)
    print("\n" + table)
    Path("bench_results.md").write_text(table + "\n")
    print("\nSaved to bench_results.md")


if __name__ == "__main__":
    asyncio.run(main())
