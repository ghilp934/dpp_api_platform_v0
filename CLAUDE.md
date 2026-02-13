# CLAUDE.md - DPP v0.4.2.2 Development Rules

## Project Context

**Project**: DPP (Decision Pack Platform) v0.4.2.2
**Goal**: Implement Agent-Centric API Platform with deterministic locks
**Stack**: Python 3.12+ / FastAPI / PostgreSQL / Redis / S3 / SQS

## NON-NEGOTIABLES (NO-GO if violated)

### A) DEC-4210 Optimistic Locking (DB-CAS)
- `runs.version` REQUIRED in all terminal state transitions
- UPDATE must include: `WHERE run_id=? AND version=?`
- If UPDATE affects 0 rows → treat as "already finalized"
- **MUST NOT** perform side-effects (settle/refund/S3 upload) if 0 rows affected

### B) DEC-4211 Money Type
- **NO float/double** for money/cost/budget
- Internal: `USD_MICROS (BIGINT)` only
- API: 4dp decimal string (e.g., `"0.0050"`)
- Use Python `Decimal` for conversions
- Reject scale > 4 with `422 INVALID_MONEY_SCALE`

### C) RFC 9457 Problem Details
- ALL errors return `application/problem+json`
- Required fields: `type`, `title`, `status`, `detail`, `instance`
- Extensions: `reason_code` (enum), `trace_id`

### D) AuthZ: Owner Guard + Stealth 404
- Run access MUST check `tenant_id`
- Non-owner access returns `404 Not Found` (NOT 403)
- `run_id` must be UUID v4 (non-guessable)

### E) URLPack SmartFetcher SSRF Defense
- Block private/loopback/link-local IPs
- HTTPS-only by default
- Redirect hop cap (max 5)
- Per-hop DNS + IP recheck
- Body size cap enforcement

### F) S3 Lifecycle
- 30-day expiration rule REQUIRED
- Abort incomplete multipart uploads (7 days)
- Code should NOT attempt orphan cleanup (infra responsibility)

## Development Workflow

### 1. Read Specs First
- Extract constants, enums, state machine rules
- Identify DB schema + invariants
- Map Redis keyspace + Lua contracts
- Review API contracts + Problem Details
- Understand finalize algorithms (Worker/Reaper winner-only)

### 2. Milestone-by-Milestone
- Each milestone MUST:
  - Run locally (docker-compose)
  - Include tests for that layer
  - Be commit-ready (small diff)
  - Pass `ruff + mypy + pytest`

### 3. No Feature Invention
- If detail missing, choose safest option + document in `DECISION_LOG.md`
- Ask user if unclear, don't guess

### 4. Testing Requirements
- Unit tests for: money conversions, Lua scripts, optimistic locking
- Integration tests: E2E flow with LocalStack
- Race tests: Worker vs Reaper simultaneous finalize (DEC-4210)
- Security tests: AuthZ, path traversal, SSRF

## Code Quality Rules

### Type Safety
- **100% type hints** (enforced by mypy strict)
- Use Pydantic BaseModel for all API schemas
- SQLAlchemy models with explicit type annotations

### No Magic Values
- Define constants in `constants.py`
- Profile values locked in `PROFILE_DPP_0_4_2_2`

### Logging
- Structured JSON logs (structlog)
- Required fields: `service`, `run_id`, `tenant_id`, `trace_id`, `version_before`, `version_after`

### Error Handling
- Custom exception hierarchy
- Map exceptions to HTTP status + reason_code
- Graceful degradation where possible

## File Organization

```
apps/api/dpp_api/
├── main.py              # FastAPI app
├── deps.py              # Dependency injection
├── routers/             # Endpoints (runs.py, health.py)
├── auth/                # API key validation
├── budget/              # Redis Lua scripts + BudgetManager
├── storage/             # S3Client
├── queue/               # SQSClient
├── db/                  # Models, repos, session
└── utils/               # money.py, hashing.py, problem_details.py
```

## Git Commit Rules

- Prefix: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`
- Include: `Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>`
- Small, focused commits (1 milestone = 1-3 commits)

## Testing Protocol

### Before Commit
```bash
ruff check apps/
mypy apps/
pytest apps/ -v
```

### Race Test (DEC-4210)
```python
def test_worker_reaper_race():
    # Simulate Worker finishing while Reaper fires
    # Assert: exactly 1 terminal transition
    # Assert: loser performs 0 side-effects
```

## Definition of Done (Release Gate)

- [ ] All tests green (unit + integration + race)
- [ ] No float usage for money (grep + mypy)
- [ ] URLPack SSRF tests pass
- [ ] Problem Details conform to RFC 9457
- [ ] Optimistic locking proven (0 rows → no side-effects)

## Key Resources

- Primary Spec: `DPP_v0_4_2_2_Detailed_Dev_Spec_for_Claude_Code_20260213.md`
- Report: `DPP_v0_4_2_2_Report_Rebuild_Integrated_20260213.md`
- Best Practices: `CONSOLIDATED_BEST_PRACTICES_v1_1_260210.md`

## Current Milestone

**MS-0**: ✅ Completed
**Next**: MS-1 (DB + AuthZ foundation)
