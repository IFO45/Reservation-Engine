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