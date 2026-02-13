"""Seed initial data for API Monetization testing.

Creates:
- 2 Plans (Basic, Premium)
- 1 Test Tenant
- 1 API Key
- Initial budget
"""

import os
import sys
import uuid

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../apps/api"))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from dpp_api.db.models import APIKey, Plan, Tenant, TenantPlan
from dpp_api.auth.api_key import hash_api_key

# Database connection
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://dpp_user:dpp_pass@localhost:5432/dpp"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def seed_data():
    """Seed initial monetization data."""
    db = SessionLocal()

    try:
        # 1. Create Plans
        print("Creating Plans...")

        basic_plan = Plan(
            plan_id="plan_basic",
            name="Basic Plan",
            status="ACTIVE",
            default_profile_version="v0.4.2.2",
            features_json={
                "allowed_pack_types": ["decision", "url"],
                "max_concurrent_runs": 5,
            },
            limits_json={
                "rate_limit_post_per_min": 10,
                "rate_limit_poll_per_min": 100,
                "pack_type_limits": {
                    "decision": {"max_cost_usd_micros": 50_000},  # $0.05
                    "url": {"max_cost_usd_micros": 100_000},  # $0.10
                },
            },
        )

        premium_plan = Plan(
            plan_id="plan_premium",
            name="Premium Plan",
            status="ACTIVE",
            default_profile_version="v0.4.2.2",
            features_json={
                "allowed_pack_types": ["decision", "url", "ocr", "video"],
                "max_concurrent_runs": 20,
            },
            limits_json={
                "rate_limit_post_per_min": 100,
                "rate_limit_poll_per_min": 1000,
                "pack_type_limits": {
                    "decision": {"max_cost_usd_micros": 500_000},  # $0.50
                    "url": {"max_cost_usd_micros": 1_000_000},  # $1.00
                    "ocr": {"max_cost_usd_micros": 2_000_000},  # $2.00
                    "video": {"max_cost_usd_micros": 5_000_000},  # $5.00
                },
            },
        )

        # Check if plans already exist
        existing_basic = db.query(Plan).filter_by(plan_id="plan_basic").first()
        if not existing_basic:
            db.add(basic_plan)
            print("[OK] Created Basic Plan")
        else:
            print("[OK] Basic Plan already exists")

        existing_premium = db.query(Plan).filter_by(plan_id="plan_premium").first()
        if not existing_premium:
            db.add(premium_plan)
            print("[OK] Created Premium Plan")
        else:
            print("[OK] Premium Plan already exists")

        # 2. Create Test Tenant
        print("\nCreating Test Tenant...")
        tenant_id = "tenant_test_001"

        existing_tenant = db.query(Tenant).filter_by(tenant_id=tenant_id).first()
        if not existing_tenant:
            tenant = Tenant(
                tenant_id=tenant_id,
                display_name="Test Tenant 001",
                status="ACTIVE",
            )
            db.add(tenant)
            db.flush()
            print(f"[OK] Created Tenant: {tenant_id}")
        else:
            print(f"[OK] Tenant {tenant_id} already exists")

        # 3. Assign Basic Plan to Tenant
        print("\nAssigning Plan to Tenant...")
        existing_assignment = (
            db.query(TenantPlan)
            .filter_by(tenant_id=tenant_id, status="ACTIVE")
            .first()
        )

        if not existing_assignment:
            from datetime import datetime, timezone

            tenant_plan = TenantPlan(
                tenant_id=tenant_id,
                plan_id="plan_basic",
                status="ACTIVE",
                effective_from=datetime.now(timezone.utc),
                effective_to=None,
                changed_by="seed_script",
                change_reason="Initial plan assignment",
            )
            db.add(tenant_plan)
            db.flush()
            print("[OK] Assigned Basic Plan to tenant")
        else:
            print("[OK] Tenant already has an active plan")

        # 4. Create API Key
        print("\nCreating API Key...")
        # P0-3: Generate API key in correct format: sk_{key_id}_{secret}
        key_id = str(uuid.uuid4())
        secret = uuid.uuid4().hex[:32]  # 32-char hex secret
        api_key_plaintext = f"sk_{key_id}_{secret}"

        existing_key = db.query(APIKey).filter_by(key_id=key_id).first()
        if not existing_key:
            key_hash = hash_api_key(api_key_plaintext)

            api_key = APIKey(
                key_id=key_id,
                tenant_id=tenant_id,
                key_hash=key_hash,
                label="Test API Key",
                status="ACTIVE",
            )
            db.add(api_key)
            db.flush()
            print(f"[OK] Created API Key: {api_key_plaintext}")
            print(f"  (Save this key - it won't be shown again)")
            print(f"  Format: sk_{{key_id}}_{{secret}}")
        else:
            print("[OK] API Key already exists")

        # 5. Initialize Budget (Redis)
        print("\nInitializing Budget...")
        from dpp_api.budget import BudgetManager
        from dpp_api.db.redis_client import RedisClient

        redis_client = RedisClient.get_client()
        budget_manager = BudgetManager(redis_client, db)

        # Check current balance
        current_balance = budget_manager.get_balance(tenant_id)
        if current_balance == 0:
            # Add $10.00 = 10,000,000 micros
            budget_manager.scripts.add_credit(tenant_id, 10_000_000)
            print(f"[OK] Added $10.00 credit to tenant {tenant_id}")
        else:
            print(f"[OK] Tenant already has balance: ${current_balance / 1_000_000:.2f}")

        # Commit all changes
        db.commit()

        print("\n" + "=" * 60)
        print("[SUCCESS] Seed data created successfully!")
        print("=" * 60)
        print(f"\nTenant ID: {tenant_id}")
        print(f"API Key: {api_key_plaintext if not existing_key else '(use existing)'}")
        print(f"Plan: Basic (10 req/min, decision=$0.05, url=$0.10)")
        print(f"Budget: ${current_balance / 1_000_000:.2f}" if current_balance else "$10.00")
        print("\nYou can now run tests with PostgreSQL!")

    except Exception as e:
        db.rollback()
        print(f"\n[ERROR] Error seeding data: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    seed_data()
