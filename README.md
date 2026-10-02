# High-Concurrency Reservation Engine 🎟️

A robust, high-throughput backend system designed to handle "flash sale" ticket reservations without overselling inventory or deadlocking the database. 

This engine solves the classic concurrent-write bottleneck by utilizing a **two-tier architecture**: an in-memory atomic gatekeeper (Redis) to absorb burst traffic, paired with an ACID-compliant durable ledger (PostgreSQL) for financial consistency and order finalization.

## 🚀 Key Features

* **Zero Overselling under High Load:** Utilizes single-threaded Redis Lua scripts to execute atomic inventory decrements, shedding excess traffic in sub-milliseconds before it touches the primary database.
* **Strict Payment Idempotency:** Mathematically prevents duplicate client charges during network retries using SHA-256 payload hashing and database-level idempotency keys.
* **Self-Healing Reconciliation:** An asynchronous background worker (`asyncpg`) continually polls for abandoned temporary holds (10-minute TTL) and automatically restores inventory to the Redis pool.
* **Non-Blocking I/O:** Built with FastAPI and `asyncpg` to bypass standard ORM overhead, allowing thousands of concurrent TCP connections with a minimal memory footprint.

## 🏗️ System Architecture

```mermaid
sequenceDiagram
    participant Client
    participant API as FastAPI (App)
    participant Redis as Redis (Gatekeeper)
    participant DB as PostgreSQL (Ledger)
    participant Worker as Background Worker

    Note over Client,DB: Phase 1: The Temporary Hold
    Client->>API: POST /api/reservations/hold (Requested Seats)
    API->>Redis: Atomic Lua Script (Check & Decrement)
    alt Insufficient Seats
        Redis-->>API: Returns 0
        API-->>Client: 409 Conflict (Sold Out)
    else Seats Available
        Redis-->>API: Returns 1 (Hold Granted)
        API->>DB: INSERT reservation (Status: PENDING, TTL: 10m)
        API-->>Client: 201 Created (reservation_id)
    end

    Note over Client,DB: Phase 2: Payment & Confirmation
    Client->>API: POST /api/reservations/confirm + Idempotency-Key
    API->>DB: SELECT Idempotency Key (Cache Check)
    alt Key Exists
        DB-->>API: Cached Response
        API-->>Client: 200 OK (Previous Receipt)
    else New Key
        API->>DB: BEGIN Transaction
        API->>DB: SELECT ... FOR UPDATE (Lock Pending Row)
        API->>DB: UPDATE events (Decrement Permanent Seats)
        API->>DB: UPDATE reservation (Status: CONFIRMED)
        API->>DB: INSERT order & idempotency receipt
        API->>DB: COMMIT
        API->>Redis: DEL temporary hold key
        API-->>Client: 200 OK (Order Confirmed)
    end

    Note over Worker,Redis: Phase 3: Background Reconciliation (Every 30s)
    Worker->>DB: UPDATE reservations SET status='EXPIRED' WHERE status='PENDING' AND expires_at <= NOW() RETURNING seats
    DB-->>Worker: List of expired seats
    Worker->>Redis: INCRBY event:available (Restore Inventory)
🛠️ Tech Stack
Framework: Python 3.12, FastAPI, Pydantic

Databases: PostgreSQL 16 (Primary Ledger), Redis 7 (Concurrency Lock & Cache)

Drivers: asyncpg (PostgreSQL), redis.asyncio (Redis)

Infrastructure: Docker, Docker Compose

💻 Local Setup & Running
Clone the repository:

Bash
git clone [https://github.com/yourusername/reservation-engine.git](https://github.com/yourusername/reservation-engine.git)
cd reservation-engine
Start the infrastructure:
The docker-compose.yml file will automatically spin up FastAPI, Redis, and PostgreSQL, and execute init.sql to seed the database tables and sample event.

Bash
docker compose up --build
Verify the API:
Navigate to http://localhost:8000/docs to view the interactive Swagger UI.

🧪 Concurrency Load Testing
To prove the system prevents race conditions, the repository includes a stress-testing script (test_concurrency.sh). It provisions exactly 10 seats in Redis and fires 50 concurrent requests at the exact same millisecond using spawned OS processes.

Run the benchmark:

Bash
chmod +x test_concurrency.sh
./test_concurrency.sh
Expected Output:

Plaintext
=== Test 2: Concurrency Stress Test (Oversell Prevention) ===
1. Resetting Redis inventory to exactly 10 seats...
2. Firing 50 concurrent requests (-P 50) for 1 seat each...
3. Collating results from 50 workers...
--------------------------------------------------
Total Requests Processed: 50 / 50
Status Code Breakdown:
     10 201
     40 409
--------------------------------------------------
[PASS] Exactly 10 requests were granted holds (HTTP 201).
[PASS] Exactly 40 requests were rejected with HTTP 409 Conflict.
4. Final Redis seats remaining: 0
[PASS] Redis counter reached exactly 0 without falling negative.
📡 API Documentation
1. Hold Seats
Reserves inventory for 10 minutes.

POST /api/reservations/hold

Body:

JSON
{
  "event_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
  "user_id": "11111111-1111-1111-1111-111111111111",
  "seats": 2
}
2. Confirm Booking
Finalizes the order and makes the inventory decrement permanent.

POST /api/reservations/confirm

Headers: Idempotency-Key: <UUID>

Body:

JSON
{
  "reservation_id": "<UUID from hold response>",
  "user_id": "11111111-1111-1111-1111-111111111111",
  "amount_cents": 5000
}

<ElicitationsGroup message="Where would you like to focus next?">
  <Elicitation label="Deploy this stack for free online" query="How can I deploy this FastAPI, Redis, and PostgreSQL setup for free online so I can include a live URL in my resume?"/>
  <Elicitation label="Start the Hybrid Search RAG project" query="Let's move on to the next resume project. Walk me through the step-by-step implementation for the Hybrid Search RAG Document API."/>
</ElicitationsGroup>