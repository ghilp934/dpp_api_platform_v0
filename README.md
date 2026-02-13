# DPP (Decision Pack Platform) v0.4.2.2

Agent-Centric API Platform for asynchronous decision pack execution.

## Overview

DPP provides a robust API platform for executing decision packs (OCR, URL analysis, Decision support, Compliance, Eval) with:

- **Async Polling**: Submit → Poll → Retrieve results
- **Budget Management**: Reserve-then-Settle with Redis Lua scripts
- **Exactly-Once Billing**: Idempotency-Key based deduplication
- **Zombie Protection**: Lease TTL + Reaper for stuck runs
- **Optimistic Locking**: DB-CAS (DEC-4210) prevents double-finalize
- **Money Type Safety**: USD_MICROS (BIGINT) eliminates float errors (DEC-4211)

## Architecture

```
┌─────────────┐
│  API (POST) │ → Reserve Budget → Create Run → Enqueue SQS
└─────────────┘
       ↓
┌─────────────┐
│   Worker    │ → Claim Lease → Execute Pack → Upload S3 → Settle → Finalize
└─────────────┘
       ↓
┌─────────────┐
│   Reaper    │ → Scan Expired → Timeout Finalize → Settle
└─────────────┘
```

## Tech Stack

- **Language**: Python 3.12+
- **API**: FastAPI + Pydantic v2
- **DB**: PostgreSQL 15+ (SQLAlchemy 2 + Alembic)
- **Cache/Budget**: Redis 7+
- **Storage**: S3-Compatible (LocalStack for dev)
- **Queue**: SQS-Compatible (LocalStack for dev)
- **Testing**: pytest + httpx

## Quick Start

### 1. Prerequisites

- Docker & Docker Compose
- Python 3.12+
- pip

### 2. Start Infrastructure

```bash
cd infra
docker-compose up -d
```

Wait for services to be healthy:
- PostgreSQL: `localhost:5432`
- Redis: `localhost:6379`
- LocalStack (S3+SQS): `localhost:4566`

### 3. Install Dependencies

```bash
pip install -e ".[dev]"
```

### 4. Run Migrations

```bash
alembic upgrade head
```

### 5. Start API Server

```bash
cd apps/api
uvicorn dpp_api.main:app --reload --host 0.0.0.0 --port 8000
```

### 6. Health Check

```bash
curl http://localhost:8000/health
```

Expected response:
```json
{
  "status": "healthy",
  "version": "0.4.2.2",
  "services": {
    "api": "up",
    "database": "up",
    "redis": "up",
    "s3": "up",
    "sqs": "up"
  }
}
```

## Development

### Run Tests

```bash
pytest apps/api/tests -v
```

### Lint & Format

```bash
ruff check apps/
black apps/
mypy apps/
```

### Project Structure

```
dpp/
├── apps/
│   ├── api/          # FastAPI application
│   ├── worker/       # SQS worker + Pack execution
│   └── reaper/       # Lease expiry scanner
├── infra/
│   ├── docker-compose.yml
│   └── localstack-init/
├── alembic/          # DB migrations
└── pyproject.toml
```

## Key Concepts

### Runs (Async Polling)

1. **Submit** (POST /v1/runs): Returns `run_id` immediately (202 Accepted)
2. **Poll** (GET /v1/runs/{run_id}): Check status (QUEUED → PROCESSING → COMPLETED/FAILED)
3. **Retrieve**: Download result from presigned S3 URL

### Money Flow

- **Reserve**: Deduct `max_cost_usd` from budget at submit
- **Settle**: Charge `actual_cost` on success, `minimum_fee` on failure
- **Refund**: Return unused amount to budget

### Optimistic Locking (DEC-4210)

Worker and Reaper compete to finalize a run. DB-CAS with `runs.version` ensures exactly-once terminal transition:

```sql
UPDATE runs
  SET status='COMPLETED', version=version+1
  WHERE run_id=? AND version=? AND status='PROCESSING';
```

If `affected_rows = 0`, the loser stops immediately (no side-effects).

### Money Type (DEC-4211)

All money values are stored as `USD_MICROS (BIGINT)`:
- Internal: `1 USD = 1,000,000 micros`
- API: `"0.5000"` (4dp decimal string)
- **No float/double** anywhere in money calculations

## Milestones

- [x] **MS-0**: Repo bootstrap + docker-compose + health endpoint
- [ ] **MS-1**: DB + AuthZ foundation
- [ ] **MS-2**: Budget engine (Redis Lua) + Money utilities
- [ ] **MS-3**: API (POST/GET runs + Idempotency + Problem Details)
- [ ] **MS-4**: Worker (SQS loop + Pack execution + Optimistic finalize)
- [ ] **MS-5**: Reaper (Lease expiry + Winner-only finalize)
- [ ] **MS-6**: Hardening & Observability

## References

- [DPP Report v0.4.2.2](../DPP_v0_4_2_2_Report_Rebuild_Integrated_20260213.md)
- [Detailed Dev Spec](../DPP_v0_4_2_2_Detailed_Dev_Spec_for_Claude_Code_20260213.md)
- [RFC 9457 Problem Details](https://www.rfc-editor.org/rfc/rfc9457)

## License

Proprietary - Internal Use Only
