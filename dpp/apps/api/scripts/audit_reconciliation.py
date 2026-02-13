#!/usr/bin/env python3
"""Reconciliation Audit Script (MS-6).

Verifies that money in DB matches money in Redis with 1 micros precision.

Checks:
  initial_balance_total = current_balance_total + reserved_total + settled_total

Where:
  - initial_balance_total: Sum of all tenants' initial balances (Redis)
  - current_balance_total: Sum of all tenants' current balances (Redis)
  - reserved_total: Sum of all active reservations (Redis)
  - settled_total: Sum of all actual_cost_usd_micros (DB, money_state='SETTLED')

Exit codes:
  0: Audit passed (perfect match)
  1: Audit failed (discrepancy found)
  2: Error (missing config, DB connection failure, etc.)
"""

import sys
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dpp_api.budget.redis_scripts import BudgetScripts
from dpp_api.db.models import Run, Tenant
from dpp_api.db.redis_client import RedisClient
from dpp_api.db.session import get_db


def get_all_tenants(db: Session) -> list[str]:
    """Get all tenant IDs from DB."""
    stmt = select(Tenant.tenant_id)
    result = db.execute(stmt)
    return [row[0] for row in result]


def get_redis_balances(
    budget_scripts: BudgetScripts, tenant_ids: list[str]
) -> dict[str, dict[str, int]]:
    """Get initial and current balances for all tenants from Redis.

    Returns:
        Dict mapping tenant_id to {"initial": int, "current": int}
    """
    balances: dict[str, dict[str, int]] = {}
    for tenant_id in tenant_ids:
        initial = budget_scripts.get_initial_balance(tenant_id)
        current = budget_scripts.get_balance(tenant_id)
        balances[tenant_id] = {"initial": initial, "current": current}
    return balances


def get_redis_reserved_total(budget_scripts: BudgetScripts) -> tuple[int, int]:
    """Get total reserved amount from all active Redis reservations.

    Returns:
        Tuple of (total_reserved_usd_micros, count)
    """
    redis_client = budget_scripts.redis
    # Scan for all reserve:* keys
    cursor = 0
    total_reserved = 0
    count = 0

    while True:
        cursor, keys = redis_client.scan(cursor, match="reserve:*", count=1000)
        for key in keys:
            # Handle both bytes and str (depends on decode_responses setting)
            if isinstance(key, bytes):
                key_str = key.decode("utf-8")
            else:
                key_str = key

            run_id = key_str.replace("reserve:", "")
            reservation = budget_scripts.get_reservation(run_id)
            if reservation:
                total_reserved += reservation["reserved_usd_micros"]
                count += 1

        if cursor == 0:
            break

    return total_reserved, count


def get_db_settled_total(db: Session) -> tuple[int, int]:
    """Get total settled amount from DB.

    Returns:
        Tuple of (total_settled_usd_micros, count)
    """
    stmt = select(
        func.sum(Run.actual_cost_usd_micros),
        func.count(Run.run_id),
    ).where(Run.money_state == "SETTLED")

    result = db.execute(stmt).one()
    total_settled = result[0] or 0  # Handle NULL case
    count = result[1] or 0

    return int(total_settled), int(count)


def format_money(usd_micros: int) -> str:
    """Format USD_MICROS as dollars for readability."""
    return f"${usd_micros / 1_000_000:.6f}"


def run_audit() -> int:
    """Run reconciliation audit.

    Returns:
        Exit code (0=pass, 1=fail, 2=error)
    """
    print("=" * 80)
    print("MS-6 RECONCILIATION AUDIT")
    print("=" * 80)
    print()

    db = None
    try:
        # Setup connections
        redis_client = RedisClient.get_client()
        budget_scripts = BudgetScripts(redis_client)

        db = next(get_db())

        # Step 1: Get all tenants
        print("[1/4] Fetching all tenants from DB...")
        tenant_ids = get_all_tenants(db)
        print(f"      Found {len(tenant_ids)} tenant(s)")
        print()

        # Step 2: Get Redis balances
        print("[2/4] Fetching Redis balances (initial + current)...")
        balances = get_redis_balances(budget_scripts, tenant_ids)

        initial_total = sum(b["initial"] for b in balances.values())
        current_total = sum(b["current"] for b in balances.values())

        print(f"      Initial balance total: {format_money(initial_total)}")
        print(f"      Current balance total: {format_money(current_total)}")
        print()

        # Step 3: Get Redis reserved total
        print("[3/4] Scanning Redis for active reservations...")
        reserved_total, reserved_count = get_redis_reserved_total(budget_scripts)
        print(f"      Reserved total: {format_money(reserved_total)} ({reserved_count} reservations)")
        print()

        # Step 4: Get DB settled total
        print("[4/4] Calculating DB settled total...")
        settled_total, settled_count = get_db_settled_total(db)
        print(f"      Settled total: {format_money(settled_total)} ({settled_count} runs)")
        print()

        # Reconciliation formula:
        # initial_total = current_total + reserved_total + settled_total
        expected_initial = current_total + reserved_total + settled_total
        discrepancy = initial_total - expected_initial

        print("=" * 80)
        print("RECONCILIATION RESULTS")
        print("=" * 80)
        print()
        print(f"  Initial balance (Redis):    {format_money(initial_total)}")
        print(f"  Current balance (Redis):    {format_money(current_total)}")
        print(f"  Reserved amount (Redis):    {format_money(reserved_total)}")
        print(f"  Settled amount (DB):        {format_money(settled_total)}")
        print()
        print(f"  Expected initial:           {format_money(expected_initial)}")
        print(f"  Actual initial:             {format_money(initial_total)}")
        print(f"  Discrepancy:                {format_money(discrepancy)}")
        print()

        if discrepancy == 0:
            print("✅ AUDIT PASSED: Money is perfectly reconciled (0 micros discrepancy)")
            print()
            return 0
        else:
            print("❌ AUDIT FAILED: Money discrepancy detected!")
            print()
            print(f"   Discrepancy: {discrepancy} USD_MICROS ({format_money(discrepancy)})")
            print()
            print("   Possible causes:")
            print("   - Initial balance not set correctly for some tenants")
            print("   - Race condition during reserve/settle operations")
            print("   - Manual Redis edits bypassing atomic operations")
            print("   - DB transaction rollback without Redis rollback")
            print()
            return 1

    except Exception as e:
        print(f"❌ ERROR: Audit failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return 2
    finally:
        if db:
            db.close()


if __name__ == "__main__":
    exit_code = run_audit()
    sys.exit(exit_code)
