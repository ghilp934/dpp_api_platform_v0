# DPP v0.4.2.2 Development Roadmap

**Project**: Decision Pack Platform API
**Version**: 0.4.2.2
**Repository**: https://github.com/ghilp934/dpp_api_platform_v0
**Last Updated**: 2026-02-13

---

## 📊 Overall Progress

| Milestone | Status | Tests | Description |
|-----------|--------|-------|-------------|
| MS-0 | ✅ **DONE** | 6/6 | Repository Bootstrap |
| MS-1 | ✅ **DONE** | 22/22 | DB + AuthZ Foundation |
| MS-2 | ✅ **DONE** | 28/28 | Budget Engine |
| MS-3 | 🔲 TODO | 0/? | API Endpoints |
| MS-4 | 🔲 TODO | 0/? | Worker |
| MS-5 | 🔲 TODO | 0/? | Reaper |
| MS-6 | 🔲 TODO | 0/? | Hardening + Production Readiness |

**Overall Completion**: 3/7 milestones (42.9%)

---

## ✅ Completed Milestones

### MS-0: Repository Bootstrap

**Status**: ✅ Complete
**Completion Date**: 2026-02-13

#### Deliverables
- [x] FastAPI application structure
- [x] Docker Compose infrastructure (PostgreSQL, Redis, LocalStack S3/SQS)
- [x] Project dependencies in `pyproject.toml`
- [x] Health check endpoints (`/health`, `/readyz`)
- [x] 6 smoke tests passing

#### Key Files
- `pyproject.toml` - Dependencies and build config
- `apps/api/dpp_api/main.py` - FastAPI app
- `apps/api/dpp_api/routers/health.py` - Health endpoints
- `infra/docker-compose.yml` - Local infrastructure
- `tests/test_smoke.py` - Basic API tests

---

### MS-1: DB + AuthZ Foundation

**Status**: ✅ Complete
**Completion Date**: 2026-02-13
**Tests**: 22/22 passing (100%)

#### Deliverables
- [x] Alembic migrations for `tenants`, `api_keys`, `runs` tables
- [x] SQLAlchemy ORM models with DEC-4210 and DEC-4211
- [x] Repository layer with optimistic locking
- [x] API Key authentication with SHA256 hashing
- [x] Owner guard with stealth 404 behavior
- [x] Comprehensive unit tests (100% coverage)

#### Technical Achievements
- **DEC-4210**: Optimistic locking with `runs.version` (BIGINT)
  - UPDATE with WHERE version=? clause
  - 0 rows affected = race loser, no side-effects
- **DEC-4211**: Money type as USD_MICROS (BIGINT)
  - All money fields: `reservation_max_cost_usd_micros`, `actual_cost_usd_micros`, `minimum_fee_usd_micros`
  - No float/double anywhere in codebase
- **2-Phase Finalize**: `finalize_token`, `finalize_stage`, `finalize_claimed_at`
- **Lease Management**: `lease_token`, `lease_expires_at` for zombie protection
- **Multi-tenancy**: `tenant_id` on all resources with owner verification

#### Key Files
- `alembic/versions/20260213_1104_*.py` - Initial schema migration
- `apps/api/dpp_api/db/models.py` - ORM models
- `apps/api/dpp_api/db/repo_*.py` - Repository layer
- `apps/api/dpp_api/auth/api_key.py` - Authentication
- `tests/test_repo_runs.py` - Repository tests (10 tests)
- `tests/test_auth.py` - Auth tests (8 tests)

---

### MS-2: Budget Engine

**Status**: ✅ Complete
**Completion Date**: 2026-02-13
**Tests**: 28/28 passing (100%)

#### Deliverables
- [x] Money utilities with DEC-4211 compliance
- [x] BudgetManager with reserve-then-settle pattern
- [x] Comprehensive unit tests (28 tests)
- [x] No float/double in money calculations

#### Technical Achievements
- **Money Utilities** (`apps/api/dpp_api/utils/money.py`)
  - [x] `usd_micros_to_decimal()` - Convert BIGINT to Decimal (4dp)
  - [x] `decimal_to_usd_micros()` - Convert Decimal to BIGINT
  - [x] `format_usd_micros()` - Format as "1.5000" string for API
  - [x] `parse_usd_string()` - Parse API string to USD_MICROS
  - [x] `validate_usd_micros()` - Range validation ($0 - $10,000)
  - [x] Uses `Decimal` for precision, no float/double
  - [x] 14 money utility tests passing

- **BudgetManager** (`apps/api/dpp_api/budget/manager.py`)
  - [x] `reserve()` - Lock maximum budget (NONE → RESERVED)
  - [x] `settle()` - Charge actual cost (RESERVED → SETTLED)
  - [x] `refund()` - Charge minimum fee on failure (RESERVED → REFUNDED)
  - [x] `get_budget_summary()` - Get current budget state
  - [x] DEC-4210 optimistic locking integrated
  - [x] State machine validation enforced
  - [x] 14 budget manager tests passing

#### Key Files
- `apps/api/dpp_api/utils/money.py` - Money conversion utilities
- `apps/api/dpp_api/budget/manager.py` - Budget management
- `tests/test_money.py` - Money utility tests (14 tests)
- `tests/test_budget.py` - Budget manager tests (14 tests)

---

## 🔲 Upcoming Milestones

---

### MS-3: API Endpoints

**Status**: 🔲 TODO
**Priority**: HIGH
**Estimated Effort**: 3-4 days

#### Objectives
Implement REST API endpoints following RFC 9457 Problem Details for errors.

#### Tasks
- [ ] **POST /api/v1/runs** - Submit new run
  - [ ] Pydantic request/response schemas
  - [ ] Idempotency key handling
  - [ ] Budget reservation
  - [ ] SQS queue submission
  - [ ] Return 202 Accepted with run_id

- [ ] **GET /api/v1/runs/{run_id}** - Poll run status
  - [ ] Owner guard with stealth 404
  - [ ] Return status, money_state, result_key
  - [ ] Terminal states: COMPLETED/FAILED/EXPIRED

- [ ] **GET /api/v1/runs/{run_id}/result** - Download result
  - [ ] Check `status = COMPLETED`
  - [ ] Generate presigned S3 URL (15min expiry)
  - [ ] Verify SHA256 hash

- [ ] **POST /api/v1/runs/{run_id}/finalize** - Worker-only finalize
  - [ ] DEC-4210 optimistic locking
  - [ ] 2-phase commit: CLAIM → side-effects → final UPDATE
  - [ ] Budget settlement
  - [ ] S3 upload verification

- [ ] **Error Handling**
  - [ ] RFC 9457 Problem Details format
  - [ ] 400: Invalid request (validation errors)
  - [ ] 401: Unauthorized (invalid API key)
  - [ ] 404: Not found (stealth for unauthorized access)
  - [ ] 409: Conflict (idempotency key mismatch)
  - [ ] 422: Unprocessable (budget exceeded)
  - [ ] 500: Internal error (with trace_id)

- [ ] **Tests** (`tests/integration/test_api.py`)
  - [ ] Full submit → poll → download flow
  - [ ] Idempotency key reuse
  - [ ] Budget validation
  - [ ] Owner guard enforcement
  - [ ] Error response formats

#### Success Criteria
- ✅ 30+ integration tests passing
- ✅ All error responses follow RFC 9457
- ✅ Idempotency working correctly
- ✅ Owner guard prevents cross-tenant access

---

### MS-4: Worker

**Status**: 🔲 TODO
**Priority**: MEDIUM
**Estimated Effort**: 3-4 days

#### Objectives
Implement async worker to process URLPack and CharPack jobs from SQS.

#### Tasks
- [ ] **Worker Main Loop** (`apps/worker/main.py`)
  - [ ] SQS long polling (20s WaitTimeSeconds)
  - [ ] Graceful shutdown (SIGTERM handling)
  - [ ] Lease acquisition and renewal (120s TTL)
  - [ ] Concurrent job processing (configurable workers)

- [ ] **URLPack SmartFetcher** (`apps/worker/packs/urlpack.py`)
  - [ ] HTTP client with timeout (30s)
  - [ ] SSRF defenses:
    - Reject private IPs (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16)
    - Reject localhost (127.0.0.0/8, ::1)
    - Reject cloud metadata (169.254.169.254)
  - [ ] Content-Type validation
  - [ ] Size limit enforcement (10MB)
  - [ ] Retry logic (3 attempts with exponential backoff)

- [ ] **CharPack Renderer** (`apps/worker/packs/charpack.py`)
  - [ ] Placeholder implementation (returns mock SVG)
  - [ ] Character validation
  - [ ] Style parameter parsing

- [ ] **Result Upload** (`apps/worker/storage/s3.py`)
  - [ ] Upload to S3 with `result_bucket/result_key`
  - [ ] SHA256 hash calculation
  - [ ] Set retention policy metadata
  - [ ] Verify upload success

- [ ] **Finalization** (`apps/worker/finalize.py`)
  - [ ] Call POST /api/v1/runs/{run_id}/finalize
  - [ ] DEC-4210 optimistic locking
  - [ ] Budget settlement (actual_cost calculation)
  - [ ] Error handling and minimum_fee charging

- [ ] **Tests** (`apps/worker/tests/`)
  - [ ] SQS message processing
  - [ ] SSRF defense tests
  - [ ] S3 upload verification
  - [ ] Finalization race conditions
  - [ ] Lease expiry handling

#### Success Criteria
- ✅ 25+ worker tests passing
- ✅ SSRF defenses working (all blacklisted IPs rejected)
- ✅ End-to-end job processing (SQS → process → S3 → finalize)
- ✅ Graceful shutdown without job loss

---

### MS-5: Reaper

**Status**: 🔲 TODO
**Priority**: LOW
**Estimated Effort**: 1-2 days

#### Objectives
Implement reaper service to detect and recover zombie runs with expired leases.

#### Tasks
- [ ] **Reaper Service** (`apps/reaper/main.py`)
  - [ ] Periodic scan (every 60s)
  - [ ] Query runs with `status=PROCESSING` and `lease_expires_at < NOW()`
  - [ ] Force timeout transition:
    - Set `status = FAILED`
    - Set `last_error_reason_code = LEASE_EXPIRED`
    - Charge `minimum_fee_usd_micros`
    - Set `money_state = SETTLED`
  - [ ] DEC-4210 optimistic locking (respect version)

- [ ] **Metrics** (`apps/reaper/metrics.py`)
  - [ ] Count of reaped runs
  - [ ] Average lease expiry duration
  - [ ] Failed reap attempts (version conflicts)

- [ ] **Tests** (`apps/reaper/tests/`)
  - [ ] Expired lease detection
  - [ ] Forced timeout transition
  - [ ] Minimum fee charging
  - [ ] Race condition with worker (version conflict)

#### Success Criteria
- ✅ 10+ reaper tests passing
- ✅ Zombie runs automatically cleaned up
- ✅ No side-effects on active runs
- ✅ Minimum fee charged on forced timeout

---

### MS-6: Hardening + Production Readiness

**Status**: 🔲 TODO
**Priority**: MEDIUM
**Estimated Effort**: 2-3 days

#### Objectives
Production-grade reliability, observability, and security hardening.

#### Tasks
- [ ] **Observability**
  - [ ] Structured logging with `structlog`
    - Add `trace_id` to all log entries
    - Log request/response for debugging
  - [ ] Metrics collection (Prometheus format)
    - Request latency histogram
    - Error rate by endpoint
    - Budget reservation/settlement counts
  - [ ] Health checks with dependency status
    - PostgreSQL: connection test
    - Redis: ping test
    - S3: bucket access test
    - SQS: queue visibility test

- [ ] **Security**
  - [ ] Rate limiting (per API key)
    - 100 requests/min per tenant
    - 429 Too Many Requests response
  - [ ] Input validation hardening
    - URL length limits (2048 chars)
    - Character whitelist for pack_type
    - Payload hash verification
  - [ ] API key rotation support
    - Multiple active keys per tenant
    - Graceful revocation

- [ ] **Reliability**
  - [ ] Database connection pooling optimization
  - [ ] SQS dead-letter queue (DLQ) handling
  - [ ] Circuit breaker for external services
  - [ ] Graceful degradation (read-only mode)

- [ ] **Configuration Management**
  - [ ] Environment-based config (dev/staging/prod)
  - [ ] Secrets management (AWS Secrets Manager integration)
  - [ ] Feature flags for gradual rollout

- [ ] **Documentation**
  - [ ] API documentation (OpenAPI/Swagger)
  - [ ] Architecture decision records (ADRs)
  - [ ] Deployment guide
  - [ ] Runbook for common issues

- [ ] **Load Testing**
  - [ ] 100 concurrent requests (p95 < 500ms)
  - [ ] 1000 runs/minute sustained
  - [ ] Database connection pool saturation
  - [ ] Worker autoscaling validation

#### Success Criteria
- ✅ p95 latency < 500ms under load
- ✅ All health checks green
- ✅ Rate limiting enforced
- ✅ Complete API documentation
- ✅ Zero-downtime deployment validated

---

## 🔑 Critical Non-Negotiables

These principles MUST be maintained across all milestones:

### DEC-4210: Optimistic Locking
```sql
UPDATE runs
SET status = 'COMPLETED', version = version + 1
WHERE run_id = ? AND version = ?
```
- **If 0 rows affected**: Loser stops immediately, no side-effects
- **Never retry**: Race losers must abort cleanly

### DEC-4211: Money Type (USD_MICROS)
```python
# ✅ CORRECT
reservation_max_cost_usd_micros: int = 1_500_000  # $1.50

# ❌ WRONG
reservation_max_cost: float = 1.50
```
- **All money in BIGINT (USD_MICROS)**
- **API uses 4dp decimal strings**: `"1.5000"`
- **No float/double in calculations**

### RFC 9457 Problem Details
```json
{
  "type": "https://dpp.example.com/errors/budget-exceeded",
  "title": "Budget Exceeded",
  "status": 422,
  "detail": "Requested cost $2.50 exceeds available budget $1.00",
  "instance": "/api/v1/runs/550e8400-e29b-41d4-a716-446655440000",
  "trace_id": "1234567890abcdef"
}
```

### AuthZ: Owner Guard + Stealth 404
```python
# ✅ CORRECT: Return 404 for unauthorized access
if run.tenant_id != auth.tenant_id:
    raise HTTPException(404, "Resource not found")

# ❌ WRONG: Reveals resource existence
if run.tenant_id != auth.tenant_id:
    raise HTTPException(403, "Forbidden")
```

---

## 📈 Success Metrics

### Code Quality
- **Test Coverage**: >90% on all modules
- **Type Coverage**: 100% (strict mypy)
- **Linting**: Zero ruff/black violations

### Performance
- **API Latency**: p95 < 500ms, p99 < 1000ms
- **Database Queries**: <5 queries per request
- **Worker Throughput**: 100+ jobs/minute per worker

### Reliability
- **Uptime**: 99.9% availability
- **Error Rate**: <0.1% of requests
- **Data Consistency**: Zero money calculation errors

---

## 🚀 Deployment Strategy

### Phase 1: MS-0 to MS-3
- **Infra**: PostgreSQL, Redis, S3, SQS (LocalStack for dev)
- **Deploy**: API server only
- **Testing**: Integration tests in staging

### Phase 2: MS-4 to MS-5
- **Infra**: Add worker and reaper services
- **Deploy**: Multi-service deployment
- **Testing**: End-to-end workflow validation

### Phase 3: MS-6
- **Infra**: Production hardening (monitoring, autoscaling)
- **Deploy**: Gradual rollout with feature flags
- **Testing**: Load testing and chaos engineering

---

## 📚 Technical Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Python | 3.12+ |
| Web Framework | FastAPI | 0.115+ |
| ORM | SQLAlchemy | 2.0+ |
| Migrations | Alembic | 1.13+ |
| Database | PostgreSQL | 15+ |
| Cache | Redis | 7+ |
| Storage | S3 (boto3) | - |
| Queue | SQS (boto3) | - |
| Validation | Pydantic | 2.9+ |
| Testing | pytest | - |
| Logging | structlog | 24.4+ |

---

## 📝 Notes

- **Windows Environment**: Static export (`output: 'export'`) is not used due to EISDIR errors
- **LocalStack**: Used for local S3/SQS development
- **Timezone**: All timestamps use `datetime.now(timezone.utc)`
- **Line Ending**: CRLF on Windows (Git auto-converts from LF)

---

**Last Updated**: 2026-02-13
**Next Review**: After MS-2 completion

For questions or clarifications, refer to:
- `CLAUDE.md` - Development rules and conventions
- `README.md` - Project overview and quick start
- Spec documents in `docs/` directory
